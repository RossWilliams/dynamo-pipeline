import { DynamoDBDocumentClient as DocumentClient } from "@aws-sdk/lib-dynamodb";
import { Key, KeyDefinition } from "./types";
declare type BatchWriteItems<KD extends KeyDefinition> = {
    tableName: string;
    records: Key<KD>[];
};
export declare class BatchWriter<KD extends KeyDefinition> {
    private client;
    private tableName;
    private activeRequests;
    private chunks;
    private nextToken;
    private retryKeys;
    private errors;
    private batchSize;
    private bufferCapacity;
    private backoffActive;
    private onUnprocessedItems;
    constructor(client: DocumentClient, items: BatchWriteItems<KD>, options: {
        onUnprocessedItems?: (keys: Key<KD>[]) => void;
        batchSize: number;
        bufferCapacity: number;
    });
    execute(): Promise<void>;
    private chunkBatchWrites;
    private writeChunk;
    private getNextChunk;
    private isActive;
    private processResult;
    private retry;
    private isDone;
    private hasNextChunk;
}
export {};
