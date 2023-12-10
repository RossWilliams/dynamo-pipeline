import { DocumentClient } from "aws-sdk/clients/dynamodb";

export abstract class AbstractFetcher<T> {
  protected activeRequests: Promise<any>[] = [];
  protected bufferSize = 0;
  protected bufferCapacity = 1;
  protected batchSize: number;
  protected limit?: number;
  protected totalReturned = 0;
  protected nextToken: number | Record<string, unknown> | null;
  protected documentClient: DocumentClient;
  protected results: T[] = [];
  protected errors: Error | null = null;

  constructor(
    client: DocumentClient,
    options: {
      batchSize: number;
      bufferCapacity: number;
      limit?: number;
    }
  ) {
    this.documentClient = client;
    this.bufferCapacity = options.bufferCapacity;
    this.batchSize = options.batchSize;
    this.limit = options.limit;
    this.nextToken = null;
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

  // take in a promise to allow recursive calls,
  // batch fetcher can immediately create many requests
  protected fetchNext(): Promise<void> | null {
    const fetchResponse = this.fetchStrategy();

    if (fetchResponse instanceof Promise && !this.activeRequests.includes(fetchResponse)) {
      return this.setupFetchProcessor(fetchResponse);
    }

    return fetchResponse;
  }

  private setupFetchProcessor(promise: Promise<any>): Promise<void> {
    this.activeRequests.push(promise);
    this.bufferSize += 1;
    return promise
      .then((data) => {
        this.activeRequests = this.activeRequests.filter((r) => r !== promise);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        this.processResult(data);
      })
      .catch((e) => {
        this.activeRequests = this.activeRequests.filter((r) => r !== promise);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        this.processError(e);
      });
  }

  // Entry point.
  async *execute(): AsyncGenerator<T[], { lastEvaluatedKey: Record<string, unknown> } | void, void> {
    let count = 0;
    do {
      if (this.errors) {
        return Promise.reject(this.errors);
      }

      if (!this.hasDataReady()) {
        await this.fetchNext();
      }

      // check for errors again after running another fetch
      if (this.errors) {
        return Promise.reject(this.errors);
      }

      const batch = this.getResultBatch(Math.min(this.batchSize, this.limit ? this.limit - count : 1000000000000));
      count += batch.length;

      if (!this.isDone() && (!this.limit || count < this.limit)) {
        // do not await here, background process the next set of data
        void this.fetchNext();
      }

      yield batch;

      if (this.limit && count >= this.limit) {
        if (typeof this.nextToken === "object" && this.nextToken !== null) {
          return { lastEvaluatedKey: this.nextToken };
        }
        return;
      }
    } while (!this.isDone());
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

  processError(e: Error): void {
    this.errors = e;
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
