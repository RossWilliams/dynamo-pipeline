import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Key, KeyDefinition } from "./types";

type BatchWriteItems<KD extends KeyDefinition> = { tableName: string; records: Key<KD>[] };

export class BatchWriter<KD extends KeyDefinition> {
  private client: DocumentClient;
  private activeRequests: Promise<any>[] = [];
  private chunks: Key<KD>[][];
  private batchSize: number = 25;
  private bufferCapacity: number = 100;
  private tableName: string;
  private errors: Error | null = null;
  private nextToken: number | null;
  private retryKeys: Key<KD>[][] | null = [];
  private backoffActive: boolean = false;
  private onUnprocessedItems: ((keys: Key<KD>[]) => void) | undefined;
  private slowStart: boolean = true;

  constructor(
    client: DocumentClient,
    items: BatchWriteItems<KD>,
    options: {
      onUnprocessedItems?: (keys: Key<KD>[]) => void;
      batchSize?: number;
      bufferCapacity?: number;
      disableSlowStart?: boolean;
    }
  ) {
    this.client = client;
    this.tableName = (items as BatchWriteItems<KD>).tableName;
    this.batchSize = options.batchSize || 25;
    this.bufferCapacity = options.bufferCapacity || this.bufferCapacity;
    this.onUnprocessedItems = options.onUnprocessedItems;
    this.chunks = this.chunkBatchWrites(items as BatchWriteItems<KD>);
    this.nextToken = 0;
    this.slowStart = !options.disableSlowStart;
  }

  async execute(): Promise<void> {
    do {
      if (this.errors) {
        return Promise.reject(this.errors);
      }

      if (!this.isDone()) {
        await this.writeChunk();

        if (this.slowStart) {
          await waitms(this.slowStart ? this.getSlowStartWait() : 1000 / this.bufferCapacity);
        }
      }
    } while (!this.isDone());

    await Promise.all(this.activeRequests);
  }

  private chunkBatchWrites(items: BatchWriteItems<KD>): Key<KD>[][] {
    const chunks = [];
    let i = 0;
    const n = items.records.length;

    while (i < n) {
      chunks.push(items.records.slice(i, (i += this.batchSize || 25)));
    }

    return chunks;
  }

  private async writeChunk(): Promise<any> {
    if (this.retryKeys && this.retryKeys.length && this.nextToken === null && !this.isActive()) {
      // if finished fetching initial requests, begin to process the retry keys
      return this.retry();
    } else if (this.activeRequests.length >= this.bufferCapacity || this.nextToken === null || this.backoffActive) {
      // return the current promise if buffer at capacity, or if there are no more items to fetch
      return this.activeRequests[0] || null;
    } else if (!this.hasNextChunk()) {
      this.nextToken = null;
      return Promise.all(this.activeRequests);
    }

    const chunk = this.getNextChunk();

    if (chunk) {
      const request = this.client.batchWrite({
        RequestItems: {
          [this.tableName]: chunk.map((item) => ({
            PutRequest: {
              Item: item,
            },
          })),
        },
      });

      if (request && typeof request.on === "function") {
        request.on("retry", (e?: { error?: { retryable?: boolean } }) => {
          if (e?.error?.retryable) {
            // reduce buffer capacity
            this.bufferCapacity = Math.max(Math.floor((this.bufferCapacity * 3) / 4), 5);
            this.backoffActive = true;
          }
        });
        request.on("complete", (e: any) => {
          if (this.backoffActive) {
            if (!e?.data?.UnprocessedItems) {
              this.backoffActive = false;
            }
          }
        });
      }

      const promise = request
        .promise()
        .catch((e) => {
          console.error("Error: AWS Error, Put Items", e);
          if (this.onUnprocessedItems) {
            this.onUnprocessedItems(chunk);
          }
          this.errors = e;
        })
        .then((results) => {
          this.processResult(results, promise);
        });

      this.activeRequests.push(promise);
    }
  }

  private getNextChunk(): Key<KD>[] | null {
    if (this.nextToken === null) {
      return null;
    }

    const chunk = this.chunks[this.nextToken] || null;

    this.nextToken += 1;

    return chunk;
  }

  private isActive(): boolean {
    return this.activeRequests.length > 0;
  }

  private processResult(data: DocumentClient.BatchWriteItemOutput | void, request: Promise<any>): void {
    this.activeRequests = this.activeRequests.filter((r) => r !== request);
    if (!this.activeRequests.length) {
      this.backoffActive = false;
    }

    if (data && data.UnprocessedItems && (data.UnprocessedItems[this.tableName]?.length || 0) > 0) {
      // eslint-disable-next-line
      const unprocessedItems = data.UnprocessedItems[this.tableName]!.map((ui) => ui.PutRequest?.Item as Key<KD>);
      if (Array.isArray(this.retryKeys)) {
        const retryItems = splitInHalf(unprocessedItems).filter(notEmpty);
        this.retryKeys.push(...retryItems);
      } else if (this.onUnprocessedItems) {
        this.onUnprocessedItems(unprocessedItems);
      }
    }
  }

  private retry(): Promise<void | null> {
    this.chunks = this.retryKeys || [];
    this.nextToken = 0;
    this.retryKeys = null;
    return this.writeChunk();
  }

  private isDone(): boolean {
    return !this.isActive() && (!this.retryKeys || this.retryKeys.length === 0) && this.nextToken === null;
  }

  private hasNextChunk(): boolean {
    if (this.nextToken === null || this.nextToken >= this.chunks.length) {
      return false;
    }

    return true;
  }

  private getSlowStartWait(): number {
    const reduction = this.bufferCapacity / 1.2; // 80% reduction from full speed
    const additional = Math.max(reduction - Math.floor((this.nextToken || 1) / 5), 0);
    return Math.min(1000 / (this.bufferCapacity - additional), 100);
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

function waitms(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
