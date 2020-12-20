import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Key } from "./types";
import { BatchFetcher } from "./AbstractFetcher";
declare type BatchGetItem = {
    keyItems: Key[];
    tableName: string;
};
declare type TransactGetItem = {
    tableName: string;
    keys: Key;
}[];
export declare class BatchGetFetcher<T> extends BatchFetcher<T> {
    private operation;
    private chunks;
    private retryKeys;
    private onUnprocessedKeys;
    constructor(client: DocumentClient, operation: "batchGet" | "transactGet", items: BatchGetItem[] | TransactGetItem[], batchSize?: number, bufferCapacity?: number, onUnprocessedKeys?: (keys: DocumentClient.KeyList) => void);
    retry(): Promise<void> | null;
    fetchStrategy(): Promise<void> | null;
    processResult(data: DocumentClient.BatchGetItemOutput | DocumentClient.TransactGetItemsOutput | void): void;
    isDone(): boolean;
    private createTransactionRequest;
    private createBatchGetRequest;
}
export {};
