import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { conditionToDynamo, skQueryToDynamoString } from "./helpers";
import { QueryFetcher } from "./QueryFetcher";
import { TableIterator } from "./TableIterator";
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

export class ScanQueryPipeline<
  PK extends string,
  SK extends string | undefined = undefined,
  KD extends { pk: PK; sk: SK } = { pk: PK; sk: SK }
> {
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
  constructor(
    tableName: string,
    keys: { pk: PK; sk?: SK },
    index?: string,
    config?: {
      client?: DocumentClient;
      readBuffer?: number;
      writeBuffer?: number;
      readBatchSize?: number;
      writeBatchSize?: number;
    }
  ) {
    this.config = {
      table: tableName,
      readBuffer: 1,
      writeBuffer: 3,
      readBatchSize: 100,
      writeBatchSize: 25,
      ...config,
      // shortcut to use KD, otherwise type definitions throughout the
      // class are too long
      keys: (keys as unknown) as KD,
      index: index,
      client: (config && config.client) || new DocumentClient(),
    };
    this.unprocessedItems = [];

    return this;
  }

  static sortKey = sortKey;
  sortKey = sortKey;

  withReadBuffer(readBuffer: number): this {
    if (readBuffer < 0) {
      throw new Error("Read buffer out of range");
    }
    this.config.readBuffer = readBuffer;
    return this;
  }

  withReadBatchSize(readBatchSize: number): this {
    if (readBatchSize < 1) {
      throw new Error("Read batch size out of range");
    }
    this.config.readBatchSize = readBatchSize;
    return this;
  }

  query<ReturnType = DocumentClient.AttributeMap>(
    keyConditions: KeyConditions<{ pk: PK; sk: SK }>,
    options?: {
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
      ...options,
    };

    return new TableIterator(new QueryFetcher<ReturnType>(request, this.config.client, "query", fetchOptions), this);
  }

  scan<ReturnType = DocumentClient.AttributeMap>(options?: {
    batchSize?: number;
    bufferCapacity?: number;
    limit?: number;
    filters?: ConditionExpression;
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
