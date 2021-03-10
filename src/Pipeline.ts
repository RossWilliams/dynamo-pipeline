import { DocumentClient } from "aws-sdk/clients/dynamodb";

import { KeyDefinition, ConditionExpression, UpdateReturnValues, PrimitiveType, Key, Scalar } from "./types";
import { BatchGetFetcher } from "./BatchFetcher";
import { TableIterator } from "./TableIterator";
import { BatchWriter } from "./BatchWriter";
import { conditionToDynamo, pkName } from "./helpers";
import { ScanQueryPipeline } from "./ScanQueryPipeline";

export class Pipeline<
  PK extends string,
  SK extends string | undefined = undefined,
  KD extends { pk: PK; sk: SK } = { pk: PK; sk: SK }
> extends ScanQueryPipeline<PK, SK, KD> {
  constructor(
    tableName: string,
    keys: { pk: PK; sk?: SK },
    config?: {
      client?: DocumentClient;
      readBuffer?: number;
      writeBuffer?: number;
      readBatchSize?: number;
      writeBatchSize?: number;
    }
  ) {
    super(tableName, keys, undefined, config);

    return this;
  }

  withWriteBuffer(writeBuffer = 30): this {
    if (writeBuffer < 0) {
      throw new Error("Write buffer out of range");
    }
    this.config.writeBuffer = writeBuffer;
    return this;
  }

  withWriteBatchSize(writeBatchSize = 25): this {
    if (writeBatchSize < 1 || writeBatchSize > 25) {
      throw new Error("Write batch size out of range");
    }
    this.config.writeBatchSize = writeBatchSize;
    return this;
  }

  createIndex<PK2 extends string, SK2 extends string>(
    name: string,
    definition: {
      pk: PK2;
      sk?: SK2;
    }
  ): ScanQueryPipeline<PK2, SK2> {
    const { keys, ...config } = this.config;
    return new ScanQueryPipeline<PK2, SK2, { pk: PK2; sk: SK2 }>(this.config.table, definition, name, config);
  }

  transactGet<T = DocumentClient.AttributeMap, KD2 extends KD = KD>(
    keys: Key<KD>[] | { tableName: string; keys: Key<KD2>; keyDefinition: KD2 }[],
    options?: {
      bufferCapacity?: number;
    }
  ): TableIterator<T, this> {
    // get keys into a standard format, filter out any non-key attributes
    const transactGetItems: { tableName: string; keys: Key<KD | KD2> }[] =
      typeof keys[0] !== "undefined" && !("tableName" in keys[0]) && !("keys" in keys[0])
        ? (keys as Key<KD>[]).map((k: Key<KD>) => ({
            tableName: this.config.table,
            keys: this.keyAttributesOnly(k, this.config.keys),
          }))
        : (keys as { tableName: string; keys: Key<KD2>; keyDefinition: KD2 }[]).map((key) => ({
            tableName: key.tableName,
            keys: this.keyAttributesOnly(key.keys, key.keyDefinition),
          }));

    return new TableIterator(
      new BatchGetFetcher<T, KD>(this.config.client, "transactGet", transactGetItems, {
        bufferCapacity: this.config.readBuffer,
        batchSize: this.config.readBatchSize,
        ...options,
      }),
      this
    );
  }

  getItems<T = DocumentClient.AttributeMap>(
    keys: Key<KD>[],
    options?: { batchSize?: number; bufferCapacity?: number }
  ): TableIterator<T, this> {
    const handleUnprocessed = (keys: Key<KD>[]) => {
      this.unprocessedItems.push(...keys);
    };

    if (typeof options?.batchSize === "number" && (options.batchSize > 100 || options.batchSize < 1)) {
      throw new Error("Batch size out of range");
    }

    if (typeof options?.bufferCapacity === "number" && options.bufferCapacity < 0) {
      throw new Error("Buffer capacity is out of range");
    }

    // filter out any non-key attributes
    const tableKeys = this.keyAttributesOnlyFromArray(keys, this.config.keys);

    const batchGetItems = { tableName: this.config.table, keys: tableKeys };

    return new TableIterator(
      new BatchGetFetcher<T, KD>(this.config.client, "batchGet", batchGetItems, {
        batchSize: this.config.readBatchSize,
        bufferCapacity: this.config.readBuffer,
        onUnprocessedKeys: handleUnprocessed,
        ...options,
      }),
      this
    );
  }

  async putItems<I extends Key<KD>>(
    items: I[],
    options?: { bufferCapacity?: number; disableSlowStart?: boolean; batchSize?: number }
  ): Promise<Pipeline<PK, SK>> {
    const handleUnprocessed = (keys: Key<KD>[]) => {
      this.unprocessedItems.push(...keys);
    };

    if (typeof options?.bufferCapacity === "number" && options.bufferCapacity < 0) {
      throw new Error("Buffer capacity is out of range");
    }

    if (typeof options?.batchSize === "number" && (options.batchSize < 1 || options.batchSize > 25)) {
      throw new Error("Batch size is out of range");
    }

    const writer = new BatchWriter(
      this.config.client,
      { tableName: this.config.table, records: items },
      {
        batchSize: this.config.writeBatchSize,
        bufferCapacity: this.config.writeBuffer,
        onUnprocessedItems: handleUnprocessed,
        ...options,
      }
    );

    await writer.execute();
    return this;
  }

  put<Item extends Key<KD>>(item: Item, condition?: ConditionExpression): Promise<Pipeline<PK, SK, KD>> {
    const request: DocumentClient.PutItemInput = {
      TableName: this.config.table,
      Item: item,
    };
    if (condition) {
      const compiledCondition = conditionToDynamo(condition);
      request.ConditionExpression = compiledCondition.Condition;
      request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
      request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
    }

    return this.config.client
      .put(request)
      .promise()
      .catch((e) => {
        console.error("Error: AWS Error, Put,", e);
        this.unprocessedItems.push(item);
      })
      .then(() => this);
  }

  putIfNotExists<Item extends Key<KD>>(item: Item): Promise<Pipeline<PK, SK>> {
    const pkCondition: ConditionExpression = {
      operator: "attribute_not_exists",
      property: pkName(this.config.keys),
    };

    return this.put(item, pkCondition);
  }

  buildUpdateRequest(
    key: Key<KD>,
    attributes: Record<string, PrimitiveType>,
    options?: {
      condition?: ConditionExpression;
      returnType?: UpdateReturnValues;
    }
  ): DocumentClient.UpdateItemInput & { UpdateExpression: string } {
    const expression = Object.keys(attributes)
      .map((k) => `#${k.replace(/#\.:/g, "")} = :${k.replace(/#\./g, "")}`)
      .join(", ");
    const expressionNames = Object.keys(attributes).reduce(
      (acc, curr) => ({ ...acc, ["#" + curr.replace(/#/g, "")]: curr }),
      {}
    );

    const expressionValues = Object.entries(attributes).reduce(
      (acc, curr) => ({
        ...acc,
        [":" + curr[0].replace(/\.:#/g, "")]: curr[1],
      }),
      {}
    );
    const request: DocumentClient.UpdateItemInput & { UpdateExpression: string } = {
      TableName: this.config.table,
      Key: this.keyAttributesOnly(key, this.config.keys),

      UpdateExpression: `SET ${expression}`,
      ...(Object.keys(expressionNames).length > 0 && {
        ExpressionAttributeNames: expressionNames,
      }),
      ...(Object.keys(expressionValues).length > 0 && {
        ExpressionAttributeValues: expressionValues,
      }),
      ...(options?.returnType && { ReturnValues: options.returnType }),
    };

    if (options?.condition) {
      const compiledCondition = conditionToDynamo(options.condition, {
        Condition: "",
        ...(Object.keys(expressionNames).length > 0 && {
          ExpressionAttributeNames: expressionNames,
        }),
        ...(Object.keys(expressionValues).length > 0 && {
          ExpressionAttributeValues: expressionValues,
        }),
      });
      request.ConditionExpression = compiledCondition.Condition;
      request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
      request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
    }

    return request;
  }

  update<T extends DocumentClient.AttributeMap>(
    key: Key<KD>,
    attributes: Record<string, PrimitiveType>,
    options?: {
      condition?: ConditionExpression;
      returnType?: UpdateReturnValues;
    }
  ): Promise<T | null> {
    // TODO: Cleanup and extact
    const request = this.buildUpdateRequest(key, attributes, options);

    return this.config.client
      .update(request)
      .promise()
      .catch((e) => {
        console.error("Error: AWS Error, Update", e);
        this.unprocessedItems.push(key);
      })
      .then((d: DocumentClient.UpdateItemOutput | Error | void) => {
        return d && "Attributes" in d && d.Attributes ? (d.Attributes as T) : null;
      });
  }

  delete<T extends DocumentClient.AttributeMap>(
    key: Key<KD>,
    options?: {
      condition?: ConditionExpression | undefined;
      returnType?: "ALL_OLD";
      reportError?: boolean;
    }
  ): Promise<T | null> {
    const request: DocumentClient.DeleteItemInput = {
      TableName: this.config.table,
      Key: this.keyAttributesOnly<KD>(key, this.config.keys),
      ...(options?.returnType && { ReturnValues: options.returnType }),
    };

    if (options?.condition) {
      const compiledCondition = conditionToDynamo(options.condition);
      request.ConditionExpression = compiledCondition.Condition;
      request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
      request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
    } else {
      const compiledCondition = conditionToDynamo({
        operator: "attribute_exists",
        property: pkName(this.config.keys),
      });
      request.ConditionExpression = compiledCondition.Condition;
      request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
      request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
    }

    return this.config.client
      .delete(request)
      .promise()
      .catch((e) => {
        if (options?.reportError) {
          console.error("Error: AWS Error, Delete", e, request);
          this.unprocessedItems.push(key);
        }
      })
      .then((old) => (old && "Attributes" in old && old.Attributes ? (old.Attributes as T) : null));
  }

  handleUnprocessed(callback: (item: Record<string, any>) => void): Pipeline<PK, SK, KD> {
    this.unprocessedItems.map(callback);
    return this;
  }

  private keyAttributesOnlyFromArray<KD extends KeyDefinition>(items: Key<KD>[], keyDefinition: KD) {
    return items.map((item) => this.keyAttributesOnly<KD>(item, keyDefinition));
  }

  private keyAttributesOnly<KD extends KeyDefinition>(item: Key<KD>, keyDefinition: KD): Key<KD> {
    return {
      [keyDefinition.pk]: item[keyDefinition.pk as KD["pk"]] as Scalar,
      ...(typeof this.config.keys.sk === "string" && {
        [this.config.keys.sk]: item[this.config.keys.sk] as Scalar,
      }),
    } as Key<KD>;
  }
}
