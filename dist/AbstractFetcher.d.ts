import { DocumentClient } from "aws-sdk/clients/dynamodb";
export declare abstract class BatchFetcher<T> {
    protected activeRequests: Promise<any>[];
    protected bufferSize: number;
    protected bufferCapacity: number;
    protected batchSize: number;
    protected limit?: number;
    protected totalReturned: number;
    protected nextToken: any | null;
    protected documentClient: DocumentClient;
    protected results: T[];
    constructor(client: DocumentClient, bufferCapacity: number, batchSize: number, limit?: number);
    abstract fetchStrategy(): Promise<void> | null;
    abstract processResult(data: Record<string, any>): void;
    protected fetchNext(promise?: Promise<any> | null): Promise<void> | null;
    execute(): AsyncGenerator<T[], void, void>;
    private setupFetchProcessor;
    getResultBatch(batchSize: number): T[];
    hasDataReady(): boolean;
    isDone(): boolean;
    isActive(): boolean;
}
