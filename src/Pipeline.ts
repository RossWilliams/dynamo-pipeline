import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { AWSError } from "aws-sdk/lib/error";
import { PromiseResult } from "aws-sdk/lib/request";

import {
  IndexDefinition,
  KeyDefinition,
  ConditionExpression,
  UpdateReturnValues,
  Operand,
  PrimitiveType,
  Key,
  LHSOperand,
  DynamoCondition,
  KeyConditions,
  SKQuery,
  SKQueryParts,
  DynamoConditionAttributeValue,
  Scalar,
} from "./types";
import { BatchGetFetcher } from "./BatchFetcher";
import { TableIterator } from "./TableIterator";
import { QueryFetcher } from "./QueryFetcher";
import { BatchWriter } from "./BatchWriter";

export class Pipeline<
  PK extends string,
  SK extends string | undefined,
  KD extends { pk: PK; sk?: SK } = { pk: PK; sk?: SK }
> {
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
  constructor(
    tableName: string,
    tableKeys: { pk: PK; sk?: SK },
    config?: {
      client?: DocumentClient;
      readBuffer?: number;
      writeBuffer?: number;
      indexes?: {
        [key: string]: KeyDefinition | undefined;
      };
    }
  ) {
    this.config = {
      table: tableName,
      indexes: {},
      // shortcut to use KD, otherwise type definitions throughout the
      // class are too long
      tableKeys: (tableKeys as unknown) as KD,
      client: (config && config.client) || new DocumentClient(),
      ...config,
    };
    this.unprocessedItems = [];
    return this;
  }

  withKeys<KD2 extends KeyDefinition>(tableKeys: { pk: KD2["pk"]; sk?: KD2["sk"] }): Pipeline<KD2["pk"], KD2["sk"]> {
    return new Pipeline<KD2["pk"], KD2["sk"]>(this.config.table, tableKeys, {
      ...this.config,
    });
  }

  withIndex(name: string, keyDefinition: KeyDefinition): Pipeline<PK, SK, KD> {
    this.config.indexes[name] = { ...keyDefinition };
    return this;
  }

  withReadBuffer(readBuffer = 4): Pipeline<PK, SK, KD> {
    this.config.readBuffer = readBuffer;
    return this;
  }

  withWriteBuffer(writeBuffer = 20): Pipeline<PK, SK, KD> {
    if (writeBuffer > 25 || writeBuffer < 1) {
      throw new Error("Write buffer out of range");
    }
    this.config.writeBuffer = writeBuffer;
    return this;
  }

  queryIndex<ReturnType = DocumentClient.AttributeMap>(
    indexName: string,
    KeyConditions: KeyConditions,
    options?: {
      batchSize?: number;
      limit?: number;
      filters?: ConditionExpression;
    }
  ): TableIterator<this, ReturnType> {
    return this.query<ReturnType>(KeyConditions, { ...options, indexName });
  }

  query<ReturnType = DocumentClient.AttributeMap>(
    keyConditions: KeyConditions,
    options?: {
      indexName?: string;
      batchSize?: number;
      limit?: number;
      filters?: ConditionExpression;
      readBuffer?: number;
    }
  ): TableIterator<this, ReturnType> {
    const request = this.buildQueryScanRequest({ ...options, keyConditions });

    const iteratorOptions = {
      readBuffer: this.config.readBuffer,
      ...options,
    };

    return new TableIterator<this, ReturnType>(
      this,
      new QueryFetcher<ReturnType>(
        request,
        this.config.client,
        "query",

        iteratorOptions
      )
    );
  }

  scanIndex<ReturnType = DocumentClient.AttributeMap>(
    indexName: string,
    options?: {
      batchSize?: number;
      limit?: number;
      filters?: ConditionExpression;
    }
  ): TableIterator<this, ReturnType> {
    return this.scan<ReturnType>({ ...options, indexName });
  }

  scan<ReturnType = DocumentClient.AttributeMap>(options?: {
    batchSize?: number;
    limit?: number;
    indexName?: string;
    filters?: ConditionExpression;
    readBuffer?: number;
  }): TableIterator<this, ReturnType> {
    const request = this.buildQueryScanRequest(options ?? {});

    const iteratorOptions = {
      bufferCapacity: this.config.readBuffer,
      ...options,
    };

    return new TableIterator<this, ReturnType>(
      this,
      new QueryFetcher<ReturnType>(request, this.config.client, "scan", iteratorOptions)
    );
  }

  transactGet<T = DocumentClient.AttributeMap, KD2 extends KD = KD>(
    keys: Key<KD>[] | { tableName: string; keys: Key<KD2>; keyDefinition: KD2 }[]
  ): TableIterator<this, T> {
    // get keys into a standard format, filter out any non-key attributes
    const transactGetItems: { tableName: string; keys: Key<KD | KD2> }[] =
      typeof keys[0] !== "undefined" && !("tableName" in keys[0]) && !("keys" in keys[0])
        ? (keys as Key<KD>[]).map((k: Key<KD>) => ({
            tableName: this.config.table,
            keys: this.keyAttributesOnly(k, this.config.tableKeys),
          }))
        : (keys as { tableName: string; keys: Key<KD2>; keyDefinition: KD2 }[]).map((key) => ({
            tableName: key.tableName,
            keys: this.keyAttributesOnly(key.keys, key.keyDefinition),
          }));

    return new TableIterator<this, T>(
      this,
      new BatchGetFetcher<T, KD>(this.config.client, "transactGet", transactGetItems, {
        bufferCapacity: this.config.readBuffer ?? 4,
      })
    );
  }

  getItems<T = DocumentClient.AttributeMap>(keys: Key<KD>[], batchSize = 100): TableIterator<this, T> {
    const handleUnprocessed = (keys: Key<KD>[]) => {
      this.unprocessedItems.push(...keys);
    };

    if (batchSize > 100 || batchSize < 1) {
      throw new Error("Batch size out of range");
    }

    // filter out any non-key attributes
    const tableKeys = this.keyAttributesOnlyFromArray(keys, this.config.tableKeys);

    const batchGetItems = { tableName: this.config.table, keys: tableKeys };

    return new TableIterator<this, T>(
      this,
      new BatchGetFetcher<T, KD>(this.config.client, "batchGet", batchGetItems, {
        batchSize,
        bufferCapacity: this.config.readBuffer,
        onUnprocessedKeys: handleUnprocessed,
      })
    );
  }

  async putItems<I extends Key<KD>>(
    items: I[],
    options?: { bufferCapacity: number; disableSlowStart?: boolean }
  ): Promise<void> {
    const handleUnprocessed = (keys: Key<KD>[]) => {
      this.unprocessedItems.push(...keys);
    };

    const writer = new BatchWriter(
      this.config.client,
      { tableName: this.config.table, records: items },
      { ...options, onUnprocessedItems: handleUnprocessed }
    );
    await writer.execute();
  }

  put(item: Record<string, any>, condition?: ConditionExpression): Promise<Pipeline<PK, SK, KD>> {
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

  putIfNotExists(item: Record<string, any>): Promise<Pipeline<PK, SK>> {
    const pkCondition: ConditionExpression = {
      operator: "attribute_not_exists",
      property: pkName(this.config.tableKeys),
    };

    return this.put(item, pkCondition);
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

    const request: DocumentClient.UpdateItemInput = {
      TableName: this.config.table,
      Key: this.keyAttributesOnly(key, this.config.tableKeys),
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
      Key: this.keyAttributesOnly<KD>(key, this.config.tableKeys),
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
        property: pkName(this.config.tableKeys),
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

  private buildQueryScanRequest(options: {
    keyConditions?: KeyConditions;
    indexName?: string;
    batchSize?: number;
    limit?: number;
    filters?: ConditionExpression;
    readBuffer?: number;
  }): DocumentClient.ScanInput | DocumentClient.QueryInput {
    let index: IndexDefinition | undefined;
    if (options.indexName) {
      const indexInConfig = this.config.indexes[options.indexName];
      if (indexInConfig) {
        index = { ...indexInConfig, name: options.indexName };
      }
    }

    if (options.indexName && typeof index === "undefined") {
      throw new Error(
        `Specified index not configured. You must specify index keys before accessing index ${options.indexName}`
      );
    }

    const request: DocumentClient.ScanInput | DocumentClient.QueryInput = {
      TableName: this.config.table,
      ...(options.limit && {
        Limit: options.limit,
      }),
      ...(index && { IndexName: index.name }),
      ...(options.keyConditions && {
        KeyConditionExpression:
          `#p0 = :v0` +
          ("sk" in options.keyConditions && options.keyConditions.sk
            ? ` AND #p1 ${skQueryToDynamoString(options.keyConditions.sk)}`
            : ""),
      }),
    };

    const [_1, skVal1, _2, skVal2] = options.keyConditions?.sk
      ? splitSkQueryToParts(options.keyConditions?.sk)
      : [undefined, undefined, undefined, undefined];

    const keySubstitues: DynamoCondition = {
      Condition: "",
      ExpressionAttributeNames: options.keyConditions
        ? {
            "#p0": index ? index.pk : this.config.tableKeys.pk,
            ...(options.keyConditions.sk && {
              "#p1": index ? (index.sk as string) : (this.config.tableKeys.sk as string),
            }),
          }
        : undefined,
      ExpressionAttributeValues: options.keyConditions
        ? {
            ":v0": options.keyConditions.pk,
            ...(typeof skVal1 !== "undefined" && {
              ":v1": skVal1,
            }),
            ...(typeof skVal2 !== "undefined" && {
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

  private keyAttributesOnlyFromArray(items: Record<string, any>[], keyDefinition: KeyDefinition) {
    return items.map((item) => this.keyAttributesOnly(item, keyDefinition));
  }

  private keyAttributesOnly<KD extends KeyDefinition>(item: Key<KD>, keyDefinition: KD): Key<KD> {
    return {
      [keyDefinition.pk]: item[keyDefinition.pk as KD["pk"]] as Scalar,
      ...(typeof this.config.tableKeys.sk === "string" && {
        [this.config.tableKeys.sk]: item[this.config.tableKeys.sk] as Scalar,
      }),
    } as Key<KD>;
  }
}

const pkName = (keys: KeyDefinition) => keys.pk;

function skQueryToDynamoString(operatorString: SKQuery): string {
  const [operator, _value1, and, value2] = splitSkQueryToParts(operatorString);
  return `${operator} :v1 ${and ? "AND" : ""} ${value2 ? ":v2" : ""}`;
}

function splitSkQueryToParts(operatorString: SKQuery): SKQueryParts {
  return operatorString.split(" ") as SKQueryParts;
}

function conditionToDynamo(
  condition: ConditionExpression | undefined,
  mergeCondition?: DynamoCondition
): DynamoCondition {
  const result: DynamoCondition =
    mergeCondition ||
    ({
      Condition: "",
    } as DynamoCondition);

  if (!condition) {
    return result;
  }

  if ("logical" in condition) {
    const preCondition = result.Condition;
    const logicalLhs = conditionToDynamo(condition.lhs, result);

    const logicalRhs = conditionToDynamo(condition.rhs, {
      Condition: preCondition,
      ExpressionAttributeNames: {
        ...result.ExpressionAttributeNames,
        ...logicalLhs.ExpressionAttributeNames,
      },
      ExpressionAttributeValues: {
        ...result.ExpressionAttributeValues,
        ...logicalLhs.ExpressionAttributeValues,
      },
    });
    if (condition.lhs && "logical" in condition.lhs) {
      logicalLhs.Condition = `(${logicalLhs.Condition})`;
    }
    if (condition.rhs && "logical" in condition.rhs) {
      logicalRhs.Condition = `(${logicalRhs.Condition})`;
    }
    result.Condition = `${logicalLhs.Condition + (logicalLhs.Condition.length ? " " : "")}${condition.logical} ${
      logicalRhs.Condition
    }`;

    Object.entries({
      ...logicalRhs.ExpressionAttributeNames,
      ...logicalLhs.ExpressionAttributeNames,
    }).forEach(([name, value]) => {
      if (!result.ExpressionAttributeNames) {
        result.ExpressionAttributeNames = {};
      }
      // @ts-expect-error: Object.entries hard codes string as the key type,
      // and indexing by template strings is invalid in ts 4.2.0
      result.ExpressionAttributeNames[name] = value;
    });

    (Object.entries<Scalar>({
      ...logicalRhs.ExpressionAttributeValues,
      ...logicalLhs.ExpressionAttributeValues,
    }) as [DynamoConditionAttributeValue, Scalar][]).forEach(([name, value]) => {
      if (!result.ExpressionAttributeValues) {
        result.ExpressionAttributeValues = {};
      }

      // @ts-expect-error:  Object.entries hard codes string as the key type
      // and indexing by template strings is invalid in ts 4.2.0
      result.ExpressionAttributeValues[name] = value;
    });

    return result;
  }

  const names = conditionToAttributeNames(
    condition,
    result.ExpressionAttributeNames ? Object.keys(result.ExpressionAttributeNames).length : 0
  );
  const values = conditionToAttributeValues(
    condition,
    result.ExpressionAttributeValues ? Object.keys(result.ExpressionAttributeValues).length : 0
  );

  const conditionString = conditionToConditionString(
    condition,
    result.ExpressionAttributeNames ? Object.keys(result.ExpressionAttributeNames).length : 0,
    result.ExpressionAttributeValues ? Object.keys(result.ExpressionAttributeValues).length : 0
  );

  return {
    ...((Object.keys(names).length > 0 || Object.keys(result.ExpressionAttributeNames || {}).length > 0) && {
      ExpressionAttributeNames: { ...names, ...result.ExpressionAttributeNames },
    }),
    ...((Object.keys(values).length > 0 || Object.keys(result.ExpressionAttributeValues || {}).length > 0) && {
      ExpressionAttributeValues: { ...values, ...result.ExpressionAttributeValues },
    }),
    Condition: conditionString,
  };
}

function comparisonOperator(
  condition: {
    lhs: LHSOperand;
    rhs: Operand;
    operator: ">" | "<" | ">=" | "<=" | "=" | "<>";
  },
  nameStart: number,
  valueStart: number
) {
  const lhs = typeof condition.lhs === "string" ? "#p" + nameStart : "#p" + nameStart;
  (typeof condition.lhs === "string" || "property" in condition.lhs) && (nameStart += 1);
  const rhs = "property" in condition.rhs ? "#p" + nameStart : ":v" + valueStart;
  return `${
    typeof condition.lhs !== "string" && "function" in condition.lhs ? condition.lhs.function + "(" : ""
  }${lhs}${typeof condition.lhs !== "string" && "function" in condition.lhs ? ")" : ""} ${condition.operator} ${
    "function" in condition.rhs ? condition.rhs.function + "(" : ""
  }${rhs}${"function" in condition.rhs ? ")" : ""}`;
}

function conditionToConditionString(
  condition: ConditionExpression,
  nameCountStart: number,
  valueCountStart: number
): string {
  // TODO: HACK: the name and value conversions follow the same operator flow
  // as the condition to values and condition to names to keep the numbers in sync
  // lhs, rhs, start,end,list
  // lhs, rhs, property, arg2
  if ("logical" in condition) {
    throw new Error("Unimplemented");
  }

  const nameStart = nameCountStart;
  let valueStart = valueCountStart;

  switch (condition.operator) {
    case ">":
    case "<":
    case ">=":
    case "<=":
    case "=":
    case "<>":
      // TODO: fix any type
      return comparisonOperator(condition as any, nameStart, valueStart);
    case "begins_with":
    case "contains":
    case "attribute_type":
      return `${condition.operator}(#p${nameStart}, :v${valueStart})`;
    case "attribute_exists":
    case "attribute_not_exists":
      return `${condition.operator}(#p${nameStart})`;
    case "between":
      return `#p${nameStart} BETWEEN :v${valueStart} AND :v${valueStart + 1}`;
    case "in":
      return `${"#p" + nameStart} IN (${condition.list
        .map(() => {
          valueStart += 1;
          return `:v${valueStart - 1}`;
        })
        .join(",")})`;
    default:
      throw new Error("Operator does not exist");
  }
}

function conditionToAttributeValues(condition: ConditionExpression, countStart = 0): { [key: string]: any } {
  const values: { [key: string]: any } = {};

  if ("rhs" in condition && condition.rhs && "value" in condition.rhs) {
    setPropertyValue(condition.rhs.value, values, countStart);
  }

  if ("value" in condition) {
    setPropertyValue(condition.value, values, countStart);
  }

  if ("start" in condition) {
    setPropertyValue(condition.start, values, countStart);
  }

  if ("end" in condition) {
    setPropertyValue(condition.end, values, countStart);
  }

  if ("list" in condition) {
    condition.list.forEach((l) => setPropertyValue(l, values, countStart));
  }

  return values;
}

function setPropertyValue(value: PrimitiveType, values: { [key: string]: any }, countStart: number) {
  // note this is the main place to change if we switch from document client to the regular dynamodb client
  const dynamoValue = Array.isArray(value)
    ? value.join("")
    : typeof value === "boolean" || typeof value === "string" || typeof value === "number"
    ? value
    : value === null
    ? true
    : value?.toString() || true;

  return setRawPropertyValue(dynamoValue, values, countStart);
}

function setRawPropertyValue(value: any, values: { [key: string]: any }, countStart: number) {
  const name = ":v" + (Object.keys(values).length + countStart);
  values[name] = value;
  return values;
}

function conditionToAttributeNames(condition: ConditionExpression, countStart = 0): { [key: string]: string } {
  const names: { [key: string]: string } = {};
  if ("lhs" in condition && condition.lhs && (typeof condition.lhs === "string" || "property" in condition.lhs)) {
    splitAndSetPropertyName(
      typeof condition.lhs === "string" ? condition.lhs : condition.lhs.property,
      names,
      countStart
    );
  }

  // TODO: Test if this is possible in a scan wih dynamo?
  if ("rhs" in condition && condition.rhs && "property" in condition.rhs) {
    splitAndSetPropertyName(condition.rhs.property, names, countStart);
  }

  if ("property" in condition) {
    splitAndSetPropertyName(condition.property, names, countStart);
  }

  return names;
}

function splitAndSetPropertyName(propertyName: string, names: { [key: string]: string }, countStart: number) {
  return propertyName.split(".").forEach((prop) => (names["#p" + (Object.keys(names).length + countStart)] = prop));
}

/*
function propToType(item: any): PropertyTypeName {
  if (typeof item === "string") {
    return "S";
  } else if (!isNaN(item)) {
    return "N";
  } else if (Array.isArray(item)) {
    return "L";
  } else if (item === null) {
    return "NULL";
  } else if (item === true || item === false) {
    return "BOOL";
  } else if (typeof item === "object" && "length" in item) {
    return "B";
  } else if (typeof item === "object") {
    return "M";
  }

  throw new Error("Type cannot be determined," + item);
}
*/
/*
function propToValue<T extends { [key: string]: any }>(item: T, name: string): AttributeValue {
  const val = name.split(".").reduce((acc, curr) => acc[curr], item);
  return {
    [propToType(val)]: val,
  };
}

function propToPrimitiveType(item: PrimitiveType): PrimitiveTypeName {
  if (typeof item === "string") {
    return "S";
  } else if (item === null) {
    return "NULL";
  } else if (item === true || item === false) {
    return "BOOL";
  } else if (typeof item === "object" && "length" in item) {
    return "B";
  } else if (!isNaN(item)) {
    return "N";
  }

  throw new Error("Type cannot be determined," + item);
}
*/

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/flat
/*
function flatten(input: any[]) {
  const stack = [...input];
  const res = [];
  while (stack.length) {
    const next = stack.pop();
    if (Array.isArray(next)) {
      // push back array items, won't modify the original input
      stack.push(...next);
    } else {
      res.push(next);
    }
  }
  // reverse to restore input order
  return res.reverse();
}
*/
