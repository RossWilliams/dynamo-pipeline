import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { ConditionExpression, UpdateReturnValues, PrimitiveType, Key } from "./types";
import { TableIterator } from "./TableIterator";
import { ScanQueryConstructorConfig, ScanQueryPipeline } from "./ScanQueryPipeline";
interface InterfaceConfig<KD> {
    client: DocumentClient;
    table: string;
    keys: KD;
    index?: string;
    readBuffer: number;
    writeBuffer: number;
    readBatchSize: number;
    writeBatchSize: number;
}
interface PipelineConstructorConfig extends ScanQueryConstructorConfig {
    client?: DocumentClient;
    readBuffer?: number;
    writeBuffer?: number;
    readBatchSize?: number;
    writeBatchSize?: number;
    writeCapacityUnitLimit?: number;
    readCapacityUnitLimit?: number;
}
export declare class Pipeline<PK extends string, SK extends string | undefined = undefined, KD extends {
    pk: PK;
    sk: SK;
} = {
    pk: PK;
    sk: SK;
}> extends ScanQueryPipeline<PK, SK, KD> {
    private writeTokenBucket?;
    config: InterfaceConfig<KD>;
    unprocessedItems: Key<KD>[];
    constructor(tableName: string, keys: {
        pk: PK;
        sk?: SK;
    }, config?: PipelineConstructorConfig);
    withWriteBuffer(writeBuffer?: number): this;
    withWriteBatchSize(writeBatchSize?: number): this;
    createIndex<PK2 extends string, SK2 extends string>(name: string, definition: {
        pk: PK2;
        sk?: SK2;
    }): ScanQueryPipeline<PK2, SK2>;
    transactGet<T = DocumentClient.AttributeMap, KD2 extends KD = KD>(keys: Key<KD>[] | {
        tableName: string;
        keys: Key<KD2>;
        keyDefinition: KD2;
    }[], options?: {
        bufferCapacity?: number;
    }): TableIterator<T, this>;
    getItems<T = DocumentClient.AttributeMap>(keys: Key<KD>[], options?: {
        batchSize?: number;
        bufferCapacity?: number;
        consistentRead?: boolean;
    }): TableIterator<T, this>;
    putItems<I extends Key<KD>>(items: I[], options?: {
        bufferCapacity?: number;
        disableSlowStart?: boolean;
        batchSize?: number;
    }): Promise<Pipeline<PK, SK>>;
    put<Item extends Key<KD>>(item: Item, condition?: ConditionExpression): Promise<Pipeline<PK, SK, KD>>;
    putIfNotExists<Item extends Key<KD>>(item: Item): Promise<Pipeline<PK, SK>>;
    buildUpdateRequest(key: Key<KD>, attributes: Record<string, PrimitiveType>, options?: {
        condition?: ConditionExpression;
        returnType?: UpdateReturnValues;
    }): DocumentClient.UpdateItemInput & {
        UpdateExpression: string;
    };
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
    private keyAttributesOnlyFromArray;
    private keyAttributesOnly;
}
export {};
