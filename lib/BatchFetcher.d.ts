import { DynamoDBDocumentClient as DocumentClient, BatchGetCommandOutput, TransactGetCommandOutput } from "@aws-sdk/lib-dynamodb";
import { Key, KeyDefinition } from "./types";
import { AbstractFetcher } from "./AbstractFetcher";
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
    });
    private chunkBatchRequests;
    retry(): Promise<void> | null;
    fetchStrategy(): Promise<void> | null;
    processResult(data: BatchGetCommandOutput | TransactGetCommandOutput | void): void;
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
