import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { KeyDefinition, ConditionExpression, UpdateReturnValues, PrimitiveType, Key, KeyConditions } from "./types";
import { TableIterator } from "./TableIterator";
import { CompoundKey } from "./types";
export declare class Pipeline<PK extends string, SK extends string | undefined = undefined, KD extends {
    pk: PK;
    sk: SK;
} = {
    pk: PK;
    sk: SK;
}> {
    config: {
        client: DocumentClient;
        table: string;
        tableKeys: KD;
        readBuffer?: number;
        writeBuffer?: number;
        readBatchSize?: number;
        writeBatchSize?: number;
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
        readBatchSize?: number;
        writeBatchSize?: number;
        indexes?: {
            [key: string]: KeyDefinition | undefined;
        };
    });
    withKeys<KD2 extends KeyDefinition>(tableKeys: {
        pk: KD2["pk"];
        sk: KD2 extends CompoundKey ? KD2["sk"] : undefined;
    }): Pipeline<KD2["pk"], KD2 extends CompoundKey ? KD2["sk"] : undefined>;
    withIndex(name: string, keyDefinition: KeyDefinition): Pipeline<PK, SK, KD>;
    withReadBuffer(readBuffer?: number): Pipeline<PK, SK, KD>;
    withWriteBuffer(writeBuffer?: number): Pipeline<PK, SK, KD>;
    withReadBatchSize(readBatchSize?: number): Pipeline<PK, SK, KD>;
    withWriteBatchSize(writeBatchSize?: number): Pipeline<PK, SK, KD>;
    queryIndex<ReturnType = DocumentClient.AttributeMap>(indexName: string, KeyConditions: KeyConditions, options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
    }): TableIterator<this, ReturnType>;
    query<ReturnType = DocumentClient.AttributeMap>(keyConditions: KeyConditions, options?: {
        indexName?: string;
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
    }): TableIterator<this, ReturnType>;
    scanIndex<ReturnType = DocumentClient.AttributeMap>(indexName: string, options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
    }): TableIterator<this, ReturnType>;
    scan<ReturnType = DocumentClient.AttributeMap>(options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        indexName?: string;
        filters?: ConditionExpression;
    }): TableIterator<this, ReturnType>;
    transactGet<T = DocumentClient.AttributeMap, KD2 extends KD = KD>(keys: Key<KD>[] | {
        tableName: string;
        keys: Key<KD2>;
        keyDefinition: KD2;
    }[], options?: {
        bufferCapacity?: number;
    }): TableIterator<this, T>;
    getItems<T = DocumentClient.AttributeMap>(keys: Key<KD>[], options?: {
        batchSize?: number;
        bufferCapacity?: number;
    }): TableIterator<this, T>;
    putItems<I extends Key<KD>>(items: I[], options?: {
        bufferCapacity?: number;
        disableSlowStart?: boolean;
        batchSize?: number;
    }): Promise<Pipeline<PK, SK>>;
    put<Item extends Key<KD>>(item: Item, condition?: ConditionExpression): Promise<Pipeline<PK, SK, KD>>;
    putIfNotExists<Item extends Key<KD>>(item: Item): Promise<Pipeline<PK, SK>>;
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
