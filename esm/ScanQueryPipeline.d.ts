import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { TableIterator } from "./TableIterator";
import { TokenBucket } from "./TokenBucket";
import { ComparisonOperator, ConditionExpression, Key, KeyConditions, QueryTemplate, Scalar } from "./types";
export declare type SortArgs = [Exclude<ComparisonOperator | "begins_with", "<>">, Scalar] | ["between", Scalar, Scalar];
export declare const sortKey: (...args: SortArgs) => QueryTemplate;
export interface ScanQueryConstructorConfig {
    /**
     * Must be an aws-sdk v2 client. A default client is created if one is not supplied.
     * Passing in a client is helpful if you need to configure authorisation or supply a custom logger.
     *
     * @default An aws-sdk v2 document client
     */
    client?: DocumentClient;
    /**
     * The maximum number of requests made in excess of the request currently being processed by the iterator.
     * Setting readBuffer to zero results in the iterator waiting for the next read's network request, while a
     * number greater than zero allows background fetching of additional items while the iterator is processing
     * previous results. Note that for batch get requests and transact get requests the read buffer also controls
     * the number of concurrent network requests. Reads are made between each iterator's request for a stride of
     * data. If a scan, query or batch fetch returns a count of items equal to or less than the batch size, the uilised
     * read buffer will not exceed 1.
     *
     * @default 1
     */
    readBuffer?: number;
    /**
     * The maximum number of items to read at once in an iterator stride. Setting a number too high will prevent
     * the read buffer from being fully utilised, but the number should be high enough for other system batched
     * operations to perform efficiently. For example, if all read items will be written back to dynamodb, a
     * batch size of 25 will ensure that a full batch write request can be utilised.
     *
     * @default 100
     */
    readBatchSize?: number;
    /**
     * The maximum number of read capacity units to be consumed over multiple iterations. Read capacity is based
     * on the reported metrics in the api response body. As such, individual requests can exceed the requested limit,
     * but subsequent requests will be throttled until there are available capacity units. If not set, no capacity
     * tracking or thottling occurs.
     *
     * @default undefined
     */
    readCapacityUnitLimit?: number;
}
interface InstanceConfig<KD> {
    client: DocumentClient;
    /**
     * Name of the table
     */
    table: string;
    /**
     * Property names of the partition key (pk) and sort key (sk)
     *
     * @default {pk: 'pk', sk: 'sk' }
     */
    keys: KD;
    /**
     * name of the GSI, if it exists
     */
    index?: string;
    /**
     * @see ScanQueryConstructorConfig["readBuffer"]
     */
    readBuffer: number;
    /**
     * @see ScanQueryConstructorConfig["readBatchSize"]
     */
    readBatchSize: number;
}
export declare class ScanQueryPipeline<PK extends string, SK extends string | undefined = undefined, KD extends {
    pk: PK;
    sk: SK;
} = {
    pk: PK;
    sk: SK;
}> {
    static sortKey: (...args: SortArgs) => QueryTemplate;
    sortKey: (...args: SortArgs) => QueryTemplate;
    config: InstanceConfig<KD>;
    /**
     * If the readCapacityUnitLimit value is set, a token bucket to track available capacity units
     */
    readTokenBucket?: TokenBucket;
    constructor(tableName: string, keys: {
        pk: PK;
        sk?: SK;
    }, index?: string, config?: ScanQueryConstructorConfig);
    protected createConfig(tableName: string, index: string | undefined, keys: KD, config?: ScanQueryConstructorConfig): InstanceConfig<KD>;
    /**
     * A convenience long-form method to set the read buffer.
     * @param readBuffer The read buffer from the constructor configuration options
     * @returns this
     */
    withReadBuffer(readBuffer: number): this;
    /**
     * A convenience long-form method to set the read batch size.
     * @param readBuffer The read batch size from the constructor configuration options
     * @returns this
     */
    withReadBatchSize(readBatchSize: number): this;
    /**
     * Prepares a dynamodb query to be executed by the returned TableIterator.
     * @param keyConditions A required partition key and optional sort key QueryTemplate.
     * The sort key should use the Pipeline.sortKey() convenience method.
     * @param options Query specific options and Pipeline configuration override options
     * @returns TableIterator
     */
    query<ReturnType = DocumentClient.AttributeMap>(keyConditions: KeyConditions<{
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
    /**
     * Prepares a dynamodb scan to be executed by the returned TableIterator.
     * @param options Scan specific options and Pipeline configuration override options
     * @returns TableIterator
     */
    scan<ReturnType = DocumentClient.AttributeMap>(options?: {
        batchSize?: number;
        bufferCapacity?: number;
        limit?: number;
        filters?: ConditionExpression;
        consistentRead?: boolean;
        nextToken?: Key<KD>;
    }): TableIterator<ReturnType, this>;
    private buildQueryScanRequest;
}
export {};
