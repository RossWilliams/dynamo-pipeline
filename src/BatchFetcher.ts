import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Key, KeyDefinition } from "./types";
import { AbstractFetcher } from "./AbstractFetcher";

type BatchGetItems<KD extends KeyDefinition> = {
  tableName: string;
  keys: Key<KD>[];
};
type TransactGetItems<KD extends KeyDefinition> = {
  tableName: string;
  keys: Key<KD>;
}[];

export class BatchGetFetcher<ReturnType, KD extends KeyDefinition> extends AbstractFetcher<ReturnType> {
  protected operation: "batchGet" | "transactGet";
  protected chunks: BatchGetItems<KD>[] | TransactGetItems<KD>[];
  protected retryKeys: BatchGetItems<KD>[] | null = [];
  protected onUnprocessedKeys: ((keys: Key<KD>[]) => void) | undefined;
  protected consistentRead = false;

  constructor(
    client: DocumentClient,
    operation: "batchGet" | "transactGet",
    items: BatchGetItems<KD> | TransactGetItems<KD>,
    options: {
      onUnprocessedKeys?: (keys: Key<KD>[]) => void;
      batchSize: number;
      bufferCapacity: number;
      consistentRead?: boolean;
    }
  ) {
    super(client, options);

    this.operation = operation;
    this.onUnprocessedKeys = options.onUnprocessedKeys;
    this.consistentRead = Boolean(options.consistentRead);

    if (operation === "batchGet" && !Array.isArray(items)) {
      this.chunks = this.chunkBatchRequests(items);
    } else {
      // Transactions don't support chunking, its a transaction
      this.chunks = [items as TransactGetItems<KD>];
    }

    this.nextToken = 0;
  }

  private chunkBatchRequests(items: BatchGetItems<KD>) {
    const chunks: BatchGetItems<KD>[] = [];
    const n = items.keys.length;
    let i = 0;
    while (i < n) {
      chunks.push({
        tableName: items.tableName,
        keys: items.keys.slice(i, (i += this.batchSize)),
      });
    }

    return chunks;
  }

  retry(): Promise<void> | null {
    this.chunks = this.retryKeys || [];
    this.nextToken = 0;
    this.retryKeys = null;
    return this.fetchNext();
    // TODO: Batch Get needs to be tested with chunk size of 1 and three items
  }

  fetchStrategy(): Promise<void> | null {
    if (this.retryKeys && this.retryKeys.length && this.nextToken === null && !this.isActive()) {
      // if finished fetching initial requests, begin to process the retry keys
      return this.retry();
    } else if (
      this.bufferSize >= this.bufferCapacity ||
      (typeof this.nextToken === "number" && this.chunks.length <= this.nextToken) ||
      this.nextToken === null
    ) {
      // return the current promise if buffer at capacity, or if there are no more items to fetch
      return this.activeRequests[0] || null;
    } else if (!this.hasNextChunk()) {
      /* istanbul ignore next */
      return null;
    }

    let promise: Promise<any> | null = null;

    if (this.operation === "transactGet") {
      const transactionRequest = this.createTransactionRequest();

      if (transactionRequest === null) {
        /* istanbul ignore next */
        return null;
      }

      promise = this.documentClient.transactGet(transactionRequest).promise();
    } else if (this.operation === "batchGet") {
      const batchGetRequest = this.createBatchGetRequest();

      if (batchGetRequest === null) {
        /* istanbul ignore next */
        return null;
      }

      promise = this.documentClient.batchGet(batchGetRequest).promise();
    }

    if (typeof this.nextToken === "number" && typeof this.chunks[this.nextToken + 1] !== "undefined") {
      this.nextToken = this.nextToken + 1;
    } else {
      this.nextToken = null;
    }

    return promise;
  }

  processResult(data: DocumentClient.BatchGetItemOutput | DocumentClient.TransactGetItemsOutput | void): void {
    let responseItems: ReturnType[] = [];
    if (data && data.Responses && Array.isArray(data.Responses)) {
      // transaction
      responseItems = data.Responses.map((r) => r.Item).filter(notEmpty) as ReturnType[];
    } else if (data && data.Responses && !Array.isArray(data.Responses)) {
      // batch, flatten each table response
      responseItems = ([] as ReturnType[])
        .concat(...(Object.values(data.Responses) as ReturnType[][]))
        .filter(notEmpty);
    }

    if (data) {
      const unprocessedKeys =
        "UnprocessedKeys" in data && (data.UnprocessedKeys as { [tableName: string]: { Keys: Key<KD>[] } });
      if (unprocessedKeys) {
        Object.entries(unprocessedKeys).forEach(([tableName, keys]) => {
          this.processError({ tableName, errorKeys: keys.Keys });
        });
      }
    }

    this.totalReturned += responseItems.length;
    this.results.push(...responseItems);
  }

  processError(err: Error | { tableName: string; errorKeys: Key<KD>[] }): void {
    if (err && "tableName" in err && Array.isArray(this.retryKeys)) {
      const retryItems = splitInHalf(err.errorKeys)
        .filter(notEmpty)
        .map((k) => ({
          tableName: err.tableName,
          keys: k,
        }));

      this.retryKeys.push(...([] as BatchGetItems<KD>[]).concat(...retryItems));
    } else if (err && "errorKeys" in err && typeof this.onUnprocessedKeys !== "undefined") {
      this.onUnprocessedKeys(err.errorKeys);
    }
  }

  isDone(): boolean {
    return super.isDone() && (!this.retryKeys || this.retryKeys.length === 0);
  }

  private createTransactionRequest(): DocumentClient.TransactGetItemsInput | null {
    const currentChunk: TransactGetItems<KD> | undefined =
      typeof this.nextToken === "number"
        ? (this.chunks[this.nextToken] as TransactGetItems<KD> | undefined)
        : undefined;

    if (!currentChunk) {
      /* istanbul ignore next */
      return null;
    }

    const transaction = {
      TransactItems: currentChunk.map((item) => ({
        Get: {
          Key: item.keys,
          TableName: item.tableName,
        },
      })),
    };

    return transaction;
  }

  // each batch handles a single table for now...
  private createBatchGetRequest(): DocumentClient.BatchGetItemInput | null {
    const currentChunk: BatchGetItems<KD> | undefined =
      typeof this.nextToken === "number" ? (this.chunks[this.nextToken] as BatchGetItems<KD> | undefined) : undefined;

    if (!currentChunk) {
      /* istanbul ignore next */
      return null;
    }
    // when multiple tables are supported in a single batch
    // switch to items.reduce(acc, curr) => ({...acc, [curr.tableName]: curr.keyItems,}),{})
    const request = {
      RequestItems: {
        [currentChunk.tableName]: {
          ConsistentRead: this.consistentRead,
          Keys: currentChunk.keys,
        },
      },
    };
    return request;
  }

  private hasNextChunk(): boolean {
    if (typeof this.nextToken !== "number" || this.nextToken >= this.chunks.length) {
      return false;
    }

    return true;
  }
}

function notEmpty<T>(val: T | null | undefined | []): val is T {
  if (Array.isArray(val) && !val.length) {
    return false;
  }

  return !!val;
}

function splitInHalf<T>(arr: T[]): T[][] {
  return [arr.slice(0, Math.ceil(arr.length / 2)), arr.slice(Math.ceil(arr.length / 2), arr.length)];
}
