import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Index, KeyType, KeySet, ConditionExpression, KeyConditions, UpdateReturnValues, PrimitiveType, Key } from "./types";
import { TableIterator } from "./TableIterator";
import { PromiseResult } from "aws-sdk/lib/request";
import { AWSError } from "aws-sdk/lib/error";
export declare class Pipeline {
    config: {
        client: DocumentClient;
        table: string;
        tableKeys: KeySet;
        readBuffer?: number;
        writeBuffer?: number;
        indexes: {
            [key: string]: Index | undefined;
        };
    };
    unprocessedItems: any[];
    constructor(table: string, config?: {
        client?: DocumentClient;
        tableKeys?: KeySet;
        readBuffer?: number;
        writeBuffer?: number;
        indexes?: {
            [key: string]: Index | undefined;
        };
    });
    withKeys(pk: string, sk?: string): Pipeline;
    withIndex(name: string, pk: string, sk?: string): Pipeline;
    withReadBuffer(readBuffer: number): Pipeline;
    withWriteBuffer(writeBuffer?: number): Pipeline;
    private buildQueryScanRequest;
    queryIndex<ReturnType = DocumentClient.AttributeMap[]>(name: string, selection: KeyConditions, batchSize?: number, limit?: number): TableIterator<ReturnType>;
    query<ReturnType = DocumentClient.AttributeMap[]>(selection: KeyConditions, index?: Index, batchSize?: number, limit?: number, filters?: ConditionExpression): TableIterator<ReturnType>;
    scanIndex<ReturnType = DocumentClient.AttributeMap[]>(name: string, batchSize: number | undefined, limit: number | undefined): TableIterator<ReturnType>;
    scan<ReturnType = DocumentClient.AttributeMap[]>(batchSize?: number, limit?: number, index?: Index, filters?: ConditionExpression): TableIterator<ReturnType>;
    transactGet<T = DocumentClient.AttributeMap[]>(keys: Key[][] | {
        tableName?: string;
        keys: Key;
    }[][] | Key[] | {
        tableName?: string;
        keys: Key;
    }[]): TableIterator<T>;
    getItems<T = DocumentClient.AttributeMap[]>(keys: Key[], batchSize?: number): TableIterator<T>;
    putItems(items: {
        [key: string]: any;
    }[]): Promise<(void | PromiseResult<DocumentClient.BatchWriteItemOutput, AWSError>)[]>;
    put(item: Record<string, any>, condition?: ConditionExpression): Promise<Pipeline>;
    putIfNotExists(item: Record<string, any>): Promise<any>;
    update<T extends DocumentClient.AttributeMap>(pk: KeyType, sk: KeyType | undefined, attributes: Record<string, PrimitiveType>, condition?: ConditionExpression | undefined, returnType?: UpdateReturnValues): Promise<T | null>;
    delete<T extends DocumentClient.AttributeMap>(pk: KeyType, sk?: KeyType | undefined, condition?: ConditionExpression | undefined, returnType?: "ALL_OLD"): Promise<T | null>;
    handleUnprocessed(callback: (item: Record<string, any>) => void): Pipeline;
}
