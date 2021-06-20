import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { TokenBucket } from "./TokenBucket";
export declare type FetchResponse = DocumentClient.QueryOutput | DocumentClient.ScanOutput | DocumentClient.BatchGetItemOutput | DocumentClient.TransactGetItemsOutput;
export declare abstract class AbstractFetcher<T> {
    protected activeRequests: Promise<any>[];
    protected bufferSize: number;
    protected bufferCapacity: number;
    protected batchSize: number;
    protected limit?: number;
    protected totalReturned: number;
    protected nextToken: number | Record<string, unknown> | null;
    protected documentClient: DocumentClient;
    protected results: T[];
    protected errors: Error | null;
    protected tokenBucket?: TokenBucket;
    constructor(client: DocumentClient, options: {
        batchSize: number;
        bufferCapacity: number;
        limit?: number;
        tokenBucket?: TokenBucket;
    });
    abstract fetchStrategy(): Promise<FetchResponse | void> | null;
    abstract processResult(data: FetchResponse | void): void;
    protected fetchNext(): Promise<FetchResponse | void> | null;
    private removeCapacityFromTokenBucket;
    private setupFetchProcessor;
    execute(): AsyncGenerator<T[], {
        lastEvaluatedKey: Record<string, unknown>;
    } | void, void>;
    getResultBatch(batchSize: number): T[];
    processError(e: Error): void;
    hasDataReady(): boolean;
    isDone(): boolean;
    isActive(): boolean;
}
