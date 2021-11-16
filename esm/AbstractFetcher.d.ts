import { DynamoDBDocumentClient as DocumentClient } from "@aws-sdk/lib-dynamodb";
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
    constructor(client: DocumentClient, options: {
        batchSize: number;
        bufferCapacity: number;
        limit?: number;
    });
    abstract fetchStrategy(): Promise<void> | null;
    abstract processResult(data: any): void;
    protected fetchNext(): Promise<void> | null;
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
