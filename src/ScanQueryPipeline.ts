import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { conditionToDynamo, skQueryToDynamoString } from "./helpers";
import { QueryFetcher } from "./QueryFetcher";
import { TableIterator } from "./TableIterator";
import { TokenBucket } from "./TokenBucket";
import {
  ComparisonOperator,
  ConditionExpression,
  DynamoCondition,
  Key,
  KeyConditions,
  QueryTemplate,
  Scalar,
} from "./types";

export type SortArgs = [Exclude<ComparisonOperator | "begins_with", "<>">, Scalar] | ["between", Scalar, Scalar];

export const sortKey = (...args: SortArgs): QueryTemplate => {
  if (args.length === 3) {
    return ["between", "and", args[1], args[2]];
  }

  return args;
};

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

export class ScanQueryPipeline<
  PK extends string,
  SK extends string | undefined = undefined,
  KD extends { pk: PK; sk: SK } = { pk: PK; sk: SK }
> {
  static sortKey = sortKey;
  sortKey = sortKey;

  config: InstanceConfig<KD>;

  /**
   * If the readCapacityUnitLimit value is set, a token bucket to track available capacity units
   */
  readTokenBucket?: TokenBucket;

  constructor(tableName: string, keys: { pk: PK; sk?: SK }, index?: string, config?: ScanQueryConstructorConfig) {
    // 'as unknown' is a shortcut to use KD, otherwise type definitions throughout the class are too long
    this.config = this.createConfig(tableName, index, (keys as unknown) as KD, config);

    if (config?.readCapacityUnitLimit) {
      this.readTokenBucket = new TokenBucket(index || tableName, config.readCapacityUnitLimit);
    }

    return this;
  }

  protected createConfig(
    tableName: string,
    index: string | undefined,
    keys: KD,
    config?: ScanQueryConstructorConfig
  ): InstanceConfig<KD> {
    return {
      table: tableName,
      readBuffer: 1,
      readBatchSize: 100,
      ...config,
      keys,
      index: index,
      client: (config && config.client) || new DocumentClient(),
    };
  }

  /**
   * A convenience long-form method to set the read buffer.
   * @param readBuffer The read buffer from the constructor configuration options
   * @returns this
   */
  withReadBuffer(readBuffer: number): this {
    if (readBuffer < 0) {
      throw new Error("Read buffer out of range");
    }
    this.config.readBuffer = readBuffer;
    return this;
  }

  /**
   * A convenience long-form method to set the read batch size.
   * @param readBuffer The read batch size from the constructor configuration options
   * @returns this
   */
  withReadBatchSize(readBatchSize: number): this {
    if (readBatchSize < 1) {
      throw new Error("Read batch size out of range");
    }
    this.config.readBatchSize = readBatchSize;
    return this;
  }

  /**
   * Prepares a dynamodb query to be executed by the returned TableIterator.
   * @param keyConditions A required partition key and optional sort key QueryTemplate.
   * The sort key should use the Pipeline.sortKey() convenience method.
   * @param options Query specific options and Pipeline configuration override options
   * @returns TableIterator
   */
  query<ReturnType = DocumentClient.AttributeMap>(
    keyConditions: KeyConditions<{ pk: PK; sk: SK }>,
    options?: {
      sortDescending?: true;
      batchSize?: number;
      bufferCapacity?: number;
      limit?: number;
      filters?: ConditionExpression;
      consistentRead?: boolean;
      nextToken?: Key<KD>;
    }
  ): TableIterator<ReturnType, this> {
    const request = this.buildQueryScanRequest({ ...options, keyConditions });

    const fetchOptions = {
      bufferCapacity: this.config.readBuffer,
      batchSize: this.config.readBatchSize,
      tokenBucket: this.readTokenBucket,
      ...options,
    };

    return new TableIterator(new QueryFetcher<ReturnType>(request, this.config.client, "query", fetchOptions), this);
  }

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
  }): TableIterator<ReturnType, this> {
    const request = this.buildQueryScanRequest(options ?? {});

    const fetchOptions = {
      bufferCapacity: this.config.readBuffer,
      batchSize: this.config.readBatchSize,
      ...options,
    };

    return new TableIterator(new QueryFetcher<ReturnType>(request, this.config.client, "scan", fetchOptions), this);
  }

  private buildQueryScanRequest(options: {
    keyConditions?: KeyConditions<KD>;
    batchSize?: number;
    limit?: number;
    filters?: ConditionExpression;
    bufferCapacity?: number;
    consistentRead?: boolean;
    sortDescending?: true;
  }): DocumentClient.ScanInput | DocumentClient.QueryInput {
    const pkName = this.config.keys.pk;
    const skName = this.config.keys.sk;

    const skValue: QueryTemplate | null =
      options.keyConditions && typeof skName !== "undefined" && options.keyConditions && skName in options.keyConditions
        ? (options.keyConditions as Record<Exclude<SK, undefined>, QueryTemplate>)[skName as Exclude<SK, undefined>]
        : null;

    const request: DocumentClient.ScanInput | DocumentClient.QueryInput = {
      TableName: this.config.table,
      ...(options.limit && {
        Limit: options.limit,
      }),
      ...(this.config.index && { IndexName: this.config.index }),
      ...(options.keyConditions && {
        KeyConditionExpression: `#p0 = :v0` + (skValue ? ` AND ${skQueryToDynamoString(skValue)}` : ""),
      }),
      ConsistentRead: Boolean(options.consistentRead),
      ScanIndexForward: Boolean(!options.sortDescending),
    };

    const [skVal1, skVal2] =
      skValue?.length === 4 ? [skValue[2], skValue[3]] : skValue?.length === 2 ? [skValue[1], null] : [null, null];

    const keySubstitues: DynamoCondition = {
      Condition: "",
      ExpressionAttributeNames: options.keyConditions
        ? {
            "#p0": pkName,
            ...(skValue && {
              "#p1": skName,
            }),
          }
        : undefined,
      ExpressionAttributeValues: options.keyConditions
        ? {
            ":v0": options.keyConditions[pkName],
            ...(skVal1 !== null && {
              ":v1": skVal1,
            }),
            ...(skVal2 !== null && {
              ":v2": skVal2,
            }),
          }
        : undefined,
    };

    if (options.filters) {
      const compiledCondition = conditionToDynamo(options.filters, keySubstitues);
      request.FilterExpression = compiledCondition.Condition;
      request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
      request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
    } else {
      request.ExpressionAttributeNames = keySubstitues.ExpressionAttributeNames;
      request.ExpressionAttributeValues = keySubstitues.ExpressionAttributeValues;
    }

    return request;
  }
}
