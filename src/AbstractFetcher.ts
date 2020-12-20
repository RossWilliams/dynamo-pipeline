import { DocumentClient } from "aws-sdk/clients/dynamodb";

export abstract class BatchFetcher<T> {
  protected activeRequests: Promise<any>[] = [];
  protected bufferSize = 0;
  protected bufferCapacity = 1;
  protected batchSize: number;
  protected limit?: number;
  protected totalReturned = 0;
  protected nextToken: any | null;
  protected documentClient: DocumentClient;
  protected results: T[] = [];

  constructor(client: DocumentClient, bufferCapacity: number, batchSize: number, limit?: number) {
    this.documentClient = client;
    this.bufferCapacity = bufferCapacity;
    this.batchSize = batchSize;
    this.limit = limit;
  }

  /*
  1. Decide if a fetch should take place considering buffer size and capacity.
  2. Perform DocumentClient operation call
  3. Set next token.
  */
  abstract fetchStrategy(): Promise<void> | null;
  /*
  1. Receive data from DocumentClient operation call in fetch strategy
  2. Set results and totalReturned.
  3. Handle API errors
  */
  abstract processResult(data: Record<string, any>): void;

  // take in a promise to allow recursive calls
  protected fetchNext(promise?: Promise<any> | null): Promise<void> | null {
    if (promise) {
      this.setupFetchProcessor(promise);
    }

    const fetchResponse = this.fetchStrategy();
    if (fetchResponse instanceof Promise && !this.activeRequests.includes(fetchResponse)) {
      this.setupFetchProcessor(fetchResponse);
    }

    return fetchResponse;
  }

  // Entry point.
  async *execute(): AsyncGenerator<T[], void, void> {
    let count = 0;
    do {
      if (!this.hasDataReady()) {
        await this.fetchNext();
      }

      const batch = this.getResultBatch(Math.min(this.batchSize, this.limit ? this.limit - count : 100000));
      count += batch.length;

      if (!this.isDone() && (!this.limit || count < this.limit)) {
        // do not await here.
        this.fetchNext();
      }

      yield batch;

      if (this.limit && count >= this.limit) {
        return;
      }
    } while (!this.isDone());
  }

  private setupFetchProcessor(promise: Promise<any>): Promise<void> {
    this.activeRequests.push(promise);
    this.bufferSize += 1;
    return promise
      .then((data) => {
        this.activeRequests = this.activeRequests.filter((r) => r !== promise);
        this.processResult(data);
      })
      .catch((e) => {
        this.activeRequests = this.activeRequests.filter((r) => r !== promise);
        console.error("Error: AWS Error,", e);
      });
  }

  getResultBatch(batchSize: number): T[] {
    const items = (this.results.length && this.results.splice(0, batchSize)) || [];

    if (!items.length) {
      this.bufferSize = this.activeRequests.length;
    } else {
      this.bufferSize -= 1;
    }

    return items;
  }

  hasDataReady(): boolean {
    return this.results.length > 0;
  }

  isDone(): boolean {
    return !this.isActive() && this.nextToken === null && this.results.length === 0;
  }

  isActive(): boolean {
    return this.activeRequests.length > 0;
  }
}
