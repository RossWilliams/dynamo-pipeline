import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Key, KeyDefinition } from "./types";
import { AbstractFetcher, FetchResponse } from "./AbstractFetcher";
import { TokenBucket } from "./TokenBucket";
declare type BatchGetItems<KD extends KeyDefinition> = {
    tableName: string;
    keys: Key<KD>[];
};
declare type TransactGetItems<KD extends KeyDefinition> = {
    tableName: string;
    keys: Key<KD>;
}[];
export declare class BatchGetFetcher<ReturnType, KD extends KeyDefinition> extends AbstractFetcher<ReturnType> {
    protected operation: "batchGet" | "transactGet";
    protected chunks: BatchGetItems<KD>[] | TransactGetItems<KD>[];
    protected retryKeys: BatchGetItems<KD>[] | null;
    protected onUnprocessedKeys: ((keys: Key<KD>[]) => void) | undefined;
    protected consistentRead: boolean;
    constructor(client: DocumentClient, operation: "batchGet" | "transactGet", items: BatchGetItems<KD> | TransactGetItems<KD>, options: {
        onUnprocessedKeys?: (keys: Key<KD>[]) => void;
        batchSize: number;
        bufferCapacity: number;
        consistentRead?: boolean;
        tokenBucket?: TokenBucket;
    });
    private chunkBatchRequests;
    retry(): Promise<FetchResponse | void> | null;
    fetchStrategy(): Promise<FetchResponse | void> | null;
    processResult(data: DocumentClient.BatchGetItemOutput | DocumentClient.TransactGetItemsOutput | void): void;
    processError(err: Error | {
        tableName: string;
        errorKeys: Key<KD>[];
    }): void;
    isDone(): boolean;
    private createTransactionRequest;
    private createBatchGetRequest;
    private hasNextChunk;
}
export {};
