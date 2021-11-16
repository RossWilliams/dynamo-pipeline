import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { NativeAttributeValue } from "@aws-sdk/util-dynamodb";
import { TableIterator } from "./TableIterator";
import { ComparisonOperator, ConditionExpression, Key, KeyConditions, QueryTemplate, Scalar } from "./types";
export declare type SortArgs = [Exclude<ComparisonOperator | "begins_with", "<>">, Scalar] | ["between", Scalar, Scalar];
export declare type AttributeMap = {
    [key: string]: NativeAttributeValue;
};
export declare const sortKey: (...args: SortArgs) => QueryTemplate;
export declare class ScanQueryPipeline<PK extends string, SK extends string | undefined = undefined, KD extends {
    pk: PK;
    sk: SK;
} = {
    pk: PK;
    sk: SK;
}> {
    config: {
        client: DynamoDBDocumentClient;
        table: string;
        keys: KD;
        index?: string;
        readBuffer: number;
        writeBuffer: number;
        readBatchSize: number;
        writeBatchSize: number;
    };
    unprocessedItems: Key<KD>[];
    constructor(tableName: string, keys: {
        pk: PK;
        sk?: SK;
    }, index?: string, config?: {
        client?: DynamoDBDocumentClient;
        readBuffer?: number;
        writeBuffer?: number;
        readBatchSize?: number;
        writeBatchSize?: number;
    });
    static sortKey: (...args: SortArgs) => QueryTemplate;
    sortKey: (...args: SortArgs) => QueryTemplate;
    withReadBuffer(readBuffer: number): this;
    withReadBatchSize(readBatchSize: number): this;
    query<ReturnType = AttributeMap>(keyConditions: KeyConditions<{
        pk: PK;
        sk: SK;
    }>, options?: {
        sortDescending?: true;
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
        consistentRead?: boolean;
        nextToken?: Key<KD>;
    }): TableIterator<ReturnType, this>;
    scan<ReturnType = AttributeMap>(options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
        consistentRead?: boolean;
        nextToken?: Key<KD>;
    }): TableIterator<ReturnType, this>;
    private buildQueryScanRequest;
}
