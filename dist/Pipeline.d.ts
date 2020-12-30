import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { AWSError } from "aws-sdk/lib/error";
import { PromiseResult } from "aws-sdk/lib/request";
import { KeyDefinition, ConditionExpression, UpdateReturnValues, PrimitiveType, Key, KeyConditions } from "./types";
import { TableIterator } from "./TableIterator";
export declare class Pipeline<PK extends string, SK extends string | undefined, KD extends {
    pk: PK;
    sk?: SK;
} = {
    pk: PK;
    sk?: SK;
}> {
    config: {
        client: DocumentClient;
        table: string;
        tableKeys: KD;
        readBuffer?: number;
        writeBuffer?: number;
        indexes: {
            [key: string]: KeyDefinition | undefined;
        };
    };
    unprocessedItems: Key<KD>[];
    constructor(tableName: string, tableKeys: {
        pk: PK;
        sk?: SK;
    }, config?: {
        client?: DocumentClient;
        readBuffer?: number;
        writeBuffer?: number;
        indexes?: {
            [key: string]: KeyDefinition | undefined;
        };
    });
    withKeys<KD2 extends KeyDefinition>(tableKeys: {
        pk: KD2["pk"];
        sk?: KD2["sk"];
    }): Pipeline<KD2["pk"], KD2["sk"]>;
    withIndex(name: string, keyDefinition: KeyDefinition): Pipeline<PK, SK, KD>;
    withReadBuffer(readBuffer?: number): Pipeline<PK, SK, KD>;
    withWriteBuffer(writeBuffer?: number): Pipeline<PK, SK, KD>;
    queryIndex<ReturnType = DocumentClient.AttributeMap>(indexName: string, KeyConditions: KeyConditions, options?: {
        batchSize?: number;
        limit?: number;
    }): TableIterator<this, ReturnType>;
    query<ReturnType = DocumentClient.AttributeMap>(keyConditions: KeyConditions, options?: {
        indexName?: string;
        batchSize?: number;
        limit?: number;
        filters?: ConditionExpression;
        readBuffer?: number;
    }): TableIterator<this, ReturnType>;
    scanIndex<ReturnType = DocumentClient.AttributeMap>(indexName: string, options?: {
        batchSize?: number;
        limit?: number;
        filters?: ConditionExpression;
    }): TableIterator<this, ReturnType>;
    scan<ReturnType = DocumentClient.AttributeMap>(options?: {
        batchSize?: number;
        limit?: number;
        indexName?: string;
        filters?: ConditionExpression;
        readBuffer?: number;
    }): TableIterator<this, ReturnType>;
    transactGet<T = DocumentClient.AttributeMap, KD2 extends KD = KD>(keys: Key<KD>[] | {
        tableName: string;
        keys: Key<KD2>;
        keyDefinition: KD2;
    }[]): TableIterator<this, T>;
    getItems<T = DocumentClient.AttributeMap>(keys: Key<KD>[], batchSize?: number): TableIterator<this, T>;
    putItems<I extends Key<KD>>(items: I[]): Promise<(void | PromiseResult<DocumentClient.BatchWriteItemOutput, AWSError>)[]>;
    put(item: Record<string, any>, condition?: ConditionExpression): Promise<Pipeline<PK, SK, KD>>;
    putIfNotExists(item: Record<string, any>): Promise<any>;
    update<T extends DocumentClient.AttributeMap>(key: Key<KD>, attributes: Record<string, PrimitiveType>, options?: {
        condition?: ConditionExpression;
        returnType?: UpdateReturnValues;
    }): Promise<T | null>;
    delete<T extends DocumentClient.AttributeMap>(key: Key<KD>, options?: {
        condition?: ConditionExpression | undefined;
        returnType?: "ALL_OLD";
        reportError?: boolean;
    }): Promise<T | null>;
    handleUnprocessed(callback: (item: Record<string, any>) => void): Pipeline<PK, SK, KD>;
    private buildQueryScanRequest;
    private keyAttributesOnlyFromArray;
    private keyAttributesOnly;
}
