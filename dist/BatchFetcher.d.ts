import { DocumentClient } from "aws-sdk/clients/dynamodb";
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
    private operation;
    private chunks;
    private retryKeys;
    private onUnprocessedKeys;
    constructor(client: DocumentClient, operation: "batchGet" | "transactGet", items: BatchGetItems<KD> | TransactGetItems<KD>, options: {
        onUnprocessedKeys?: (keys: Key<KD>[]) => void;
        batchSize?: number;
        bufferCapacity?: number;
    });
    retry(): Promise<void> | null;
    fetchStrategy(): Promise<void> | null;
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
