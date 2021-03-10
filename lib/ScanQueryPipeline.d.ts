import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { TableIterator } from "./TableIterator";
import { ComparisonOperator, ConditionExpression, Key, KeyConditions, QueryTemplate, Scalar } from "./types";
export declare type SortArgs = [Exclude<ComparisonOperator, "<>">, Scalar] | ["between", Scalar, Scalar];
export declare const sortKey: (...args: SortArgs) => QueryTemplate;
export declare class ScanQueryPipeline<PK extends string, SK extends string | undefined = undefined, KD extends {
    pk: PK;
    sk: SK;
} = {
    pk: PK;
    sk: SK;
}> {
    config: {
        client: DocumentClient;
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
        client?: DocumentClient;
        readBuffer?: number;
        writeBuffer?: number;
        readBatchSize?: number;
        writeBatchSize?: number;
    });
    withReadBuffer(readBuffer: number): this;
    withReadBatchSize(readBatchSize: number): this;
    query<ReturnType = DocumentClient.AttributeMap>(keyConditions: KeyConditions<{
        pk: PK;
        sk: SK;
    }>, options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
        consistentRead?: boolean;
    }): TableIterator<ReturnType, this>;
    scan<ReturnType = DocumentClient.AttributeMap>(options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
    }): TableIterator<ReturnType, this>;
    private buildQueryScanRequest;
}
