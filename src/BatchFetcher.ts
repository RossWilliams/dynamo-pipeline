import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Key } from "./types";
import { BatchFetcher } from "./AbstractFetcher";

type BatchGetItem = { keyItems: Key[]; tableName: string };
type TransactGetItem = { tableName: string; keys: Key }[];

export class BatchGetFetcher<T> extends BatchFetcher<T> {
  private operation: "batchGet" | "transactGet";
  private chunks: BatchGetItem[] | TransactGetItem[];
  private retryKeys: BatchGetItem[] | null = [];
  private onUnprocessedKeys: ((keys: DocumentClient.KeyList) => void) | undefined;

  constructor(
    client: DocumentClient,
    operation: "batchGet" | "transactGet",
    items: BatchGetItem[] | TransactGetItem[],
    batchSize = 100,
    bufferCapacity = 4,
    onUnprocessedKeys?: (keys: DocumentClient.KeyList) => void
  ) {
    super(client, bufferCapacity, batchSize);

    this.operation = operation;
    this.onUnprocessedKeys = onUnprocessedKeys;

    if (operation === "batchGet") {
      const chunks: BatchGetItem[] = [];
      (items as BatchGetItem[]).forEach((item) => {
        let i = 0;
        const n = item.keyItems.length;
        while (i < n) {
          chunks.push({ tableName: item.tableName, keyItems: item.keyItems.slice(i, (i += batchSize)) });
        }
      });
      this.chunks = chunks;
    } else {
      // TODO: transactions don't support chunking yet.
      this.chunks = items;
    }

    this.nextToken = 0;
  }

  retry(): Promise<void> | null {
    this.chunks = this.retryKeys || [];
    this.nextToken = 0;
    this.retryKeys = null;
    return this.fetchNext();
  }

  fetchStrategy(): Promise<void> | null {
    if (this.retryKeys && this.retryKeys.length && this.nextToken === null && !this.isActive()) {
      // if finished fetching initial requests, begine to process the retry keys
      return this.retry();
    } else if (
      this.bufferSize >= this.bufferCapacity ||
      this.chunks.length <= this.nextToken ||
      this.nextToken === null
    ) {
      // return the current promise if buffer at capacity, or if there are no more items to fetch
      return this.activeRequests[0] || null;
    }

    let promise: Promise<any> | null = null;

    if (this.operation === "transactGet") {
      promise = this.documentClient
        .transactGet(this.createTransactionRequest(this.chunks[this.nextToken] as TransactGetItem))
        .promise();
    } else if (this.operation === "batchGet") {
      promise = this.documentClient
        .batchGet(this.createBatchGetRequest(this.chunks[this.nextToken] as BatchGetItem))
        .promise();
    }

    this.nextToken = typeof this.chunks[this.nextToken + 1] !== "undefined" ? this.nextToken + 1 : null;

    return this.fetchNext(promise);
  }

  processResult(data: DocumentClient.BatchGetItemOutput | DocumentClient.TransactGetItemsOutput | void): void {
    let responseItems: T[] = [];
    if (data && data.Responses && Array.isArray(data.Responses)) {
      // transaction
      responseItems = data.Responses.map((r) => r.Item).filter(notEmpty) as T[];
    } else if (data && data.Responses && !Array.isArray(data.Responses)) {
      // batch, flatten each table response
      responseItems = ([] as T[]).concat(...(Object.values(data.Responses) as T[][])).filter(notEmpty);
    }

    if (data) {
      const unprocessedKeys = "UnprocessedKeys" in data && data.UnprocessedKeys;
      if (unprocessedKeys && Array.isArray(this.retryKeys)) {
        const retryItems = Object.entries(unprocessedKeys).map(([tableName, keys]) =>
          splitInHalf(keys.Keys)
            .filter(notEmpty)
            .map((k) => ({
              tableName,
              keyItems: k,
            }))
        );
        this.retryKeys.push(...([] as BatchGetItem[]).concat(...retryItems));
      } else if (unprocessedKeys && typeof this.onUnprocessedKeys !== "undefined") {
        const iter = this.onUnprocessedKeys.bind(this);
        Object.entries(unprocessedKeys).forEach(([_tableName, keys]) => iter(keys.Keys));
      }
    }

    this.totalReturned += responseItems.length;
    this.results.push(...responseItems);
  }

  isDone(): boolean {
    return super.isDone() && (!this.retryKeys || this.retryKeys.length === 0);
  }

  private createTransactionRequest(items: TransactGetItem) {
    return {
      TransactItems: items.map((item) => ({
        Get: {
          Key: item.keys,
          TableName: item.tableName,
        },
      })),
    };
  }

  // each batch handles a single table for now...
  private createBatchGetRequest(items: BatchGetItem): DocumentClient.BatchGetItemInput {
    // when multiple tables are supported in a single batch
    // switch to items.reduce(acc, curr) => ({...acc, [curr.tableName]: curr.keyItems,}),{})
    const request = {
      RequestItems: {
        [items.tableName]: {
          Keys: items.keyItems,
        },
      },
    };
    return request;
  }
}

function notEmpty<T>(val: T | null | undefined): val is T {
  return !!val;
}

function splitInHalf<T>(arr: T[]): T[][] {
  return [arr.slice(0, Math.ceil(arr.length / 2)), arr.slice(Math.ceil(arr.length / 2), arr.length)];
}
