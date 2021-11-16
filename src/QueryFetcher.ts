import { AbstractFetcher } from "./AbstractFetcher";

import {
  DynamoDBDocumentClient as DocumentClient,
  ScanCommandOutput,
  QueryCommandOutput,
  QueryCommand,
  ScanCommand,
  ScanCommandInput,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

export class QueryFetcher<T> extends AbstractFetcher<T> {
  private request: ScanCommandInput | QueryCommandInput;
  private operation: "query" | "scan";

  constructor(
    request: ScanCommandInput | QueryCommandInput,
    client: DocumentClient,
    operation: "query" | "scan",
    options: {
      batchSize: number;
      bufferCapacity: number;
      limit?: number;
      nextToken?: Record<string, unknown>;
    }
  ) {
    super(client, options);
    this.request = request;
    this.operation = operation;
    if (options.nextToken) {
      this.nextToken = options.nextToken;
    } else {
      this.nextToken = 1;
    }
  }

  // TODO: remove null response type
  fetchStrategy(): null | Promise<any> {
    // no support for parallel query
    // 1. 1 active request allowed at a time
    // 2. Do not create a new request when the buffer is full
    // 3. If there are no more items to fetch, exit
    if (this.activeRequests.length > 0 || this.bufferSize > this.bufferCapacity || !this.nextToken) {
      return this.activeRequests[0] || null;
    }

    const request = {
      ...(this.request.Limit && { Limit: this.request.Limit - this.totalReturned }),
      ...this.request,
      ...(Boolean(this.nextToken) && typeof this.nextToken === "object" && { ExclusiveStartKey: this.nextToken }),
    };

    if (this.operation === "query") {
      return this.documentClient.send(new QueryCommand(request));
    }

    return this.documentClient.send(new ScanCommand(request));
  }

  processResult(data: ScanCommandOutput | QueryCommandOutput | void): void {
    this.nextToken = (data && data.LastEvaluatedKey) || null;

    if (data && data.Items) {
      this.totalReturned += data.Items.length;
      this.results.push(...(data.Items as T[]));
    }
  }

  // override since filtering results in inconsistent result set size, base buffer on the items returned last
  // this may give surprising results if the returned list varies considerably, but errs on the side of caution.
  getResultBatch(batchSize: number): T[] {
    const items = super.getResultBatch(batchSize);

    if (items.length > 0) {
      this.bufferSize = this.results.length / items.length;
    } else if (!this.activeRequests.length) {
      // if we don't have any items to process, and no active requests, buffer size should be zero.
      this.bufferSize = 0;
    }

    return items;
  }
}
