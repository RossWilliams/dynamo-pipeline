"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Pipeline = void 0;
const dynamodb_1 = require("aws-sdk/clients/dynamodb");
const BatchFetcher_1 = require("./BatchFetcher");
const TableIterator_1 = require("./TableIterator");
const QueryFetcher_1 = require("./QueryFetcher");
class Pipeline {
    constructor(table, config) {
        this.config = {
            table,
            indexes: {},
            tableKeys: { pk: "id" },
            client: (config && config.client) || new dynamodb_1.DocumentClient(),
            ...config,
        };
        this.unprocessedItems = [];
        return this;
    }
    // defaults to keys of type string
    withKeys(pk, sk) {
        this.config.tableKeys = {
            pk,
            sk,
        };
        return this;
    }
    withIndex(name, pk, sk) {
        this.config.indexes[name] = { pk, sk, name };
        return this;
    }
    withReadBuffer(readBuffer) {
        this.config.readBuffer = readBuffer;
        return this;
    }
    withWriteBuffer(writeBuffer = 20) {
        if (writeBuffer > 25 || writeBuffer < 1) {
            throw new Error("Write buffer out of range");
        }
        this.config.writeBuffer = writeBuffer;
        return this;
    }
    buildQueryScanRequest(index, limit = 0, keyConditions, filters) {
        const request = {
            TableName: this.config.table,
            ...(limit && {
                Limit: limit,
            }),
            ...(index && { IndexName: index.name }),
            ...(keyConditions && {
                // TODO: Names and values needed for escaping
                KeyConditionExpression: `#p0 = :v0` + (keyConditions.sk ? `AND ${operatorToSymbol(keyConditions.skOperator || "=")}` : ""),
            }),
        };
        const keySubstitues = {
            Condition: "",
            ExpressionAttributeNames: keyConditions
                ? {
                    "#p0": index ? index.pk : this.config.tableKeys.pk,
                    ...(keyConditions.sk && {
                        "#p1": index ? index.sk : this.config.tableKeys.sk,
                    }),
                }
                : undefined,
            ExpressionAttributeValues: keyConditions
                ? {
                    ":v0": keyConditions.pk,
                    ...(keyConditions.sk && {
                        ":v1": keyConditions.sk,
                    }),
                    ...(keyConditions.sk2 && {
                        ":v2": keyConditions.sk2,
                    }),
                }
                : undefined,
        };
        if (filters) {
            // TODO: When key conditions names and values are escaped, this needs updating
            const compiledCondition = conditionToDynamo(filters, keySubstitues);
            request.FilterExpression = compiledCondition.Condition;
            request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
            request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
        }
        else {
            request.ExpressionAttributeNames = keySubstitues.ExpressionAttributeNames;
            request.ExpressionAttributeValues = keySubstitues.ExpressionAttributeValues;
        }
        return request;
    }
    queryIndex(name, selection, batchSize, limit) {
        const index = this.config.indexes[name];
        if (!index) {
            throw new Error("Index not found: " + name);
        }
        return this.query(selection, index, batchSize, limit);
    }
    query(selection, index, batchSize, limit, filters) {
        var _a;
        const request = this.buildQueryScanRequest(index, limit, selection, filters);
        return new TableIterator_1.TableIterator(this, new QueryFetcher_1.QueryFetcher(request, this.config.client, "query", batchSize, (_a = this.config.readBuffer) !== null && _a !== void 0 ? _a : 4, limit));
    }
    scanIndex(name, batchSize, limit) {
        const index = this.config.indexes[name];
        if (!index) {
            throw new Error("Index not found: " + name);
        }
        return this.scan(batchSize, limit, index);
    }
    scan(batchSize, limit, index, filters) {
        var _a;
        const request = this.buildQueryScanRequest(index, limit, undefined, filters);
        return new TableIterator_1.TableIterator(this, new QueryFetcher_1.QueryFetcher(request, this.config.client, "scan", batchSize, (_a = this.config.readBuffer) !== null && _a !== void 0 ? _a : 4, limit));
    }
    transactGet(keys) {
        var _a;
        const handleUnprocessed = (keys) => {
            this.unprocessedItems.push(...keys);
        };
        const normalise = (key) => "tableName" in key && typeof key.keys === "object" && !Array.isArray(key.keys)
            ? key
            : typeof key.keys === "object" && !Array.isArray(key.keys)
                ? { tableName: this.config.table, keys: key.keys }
                : { tableName: this.config.table, keys: key };
        const transactGetItems = [];
        keys.forEach((keySet) => {
            // multiple transact gets
            if (Array.isArray(keySet)) {
                const cluster = [];
                keySet.forEach((key) => {
                    const item = normalise(key);
                    cluster.push(item);
                });
                transactGetItems.push(cluster);
            }
            else {
                // single transact get
                transactGetItems[0] = transactGetItems[0] || [];
                transactGetItems[0].push(normalise(keySet));
            }
        });
        return new TableIterator_1.TableIterator(this, new BatchFetcher_1.BatchGetFetcher(this.config.client, "transactGet", transactGetItems, undefined, (_a = this.config.readBuffer) !== null && _a !== void 0 ? _a : 4, handleUnprocessed));
    }
    getItems(keys, batchSize = 100) {
        var _a;
        const handleUnprocessed = (keys) => {
            this.unprocessedItems.push(...keys);
        };
        if (batchSize > 100 || batchSize < 1) {
            throw new Error("Batch size out of range");
        }
        // filter out any non-key attributes
        const tableKeys = keys.map((k) => ({
            [this.config.tableKeys.pk]: k[this.config.tableKeys.pk],
            ...(this.config.tableKeys.sk && { [this.config.tableKeys.sk]: k[this.config.tableKeys.sk] }),
        }));
        const batchGetItems = [{ tableName: this.config.table, keyItems: tableKeys }];
        return new TableIterator_1.TableIterator(this, new BatchFetcher_1.BatchGetFetcher(this.config.client, "batchGet", batchGetItems, batchSize, (_a = this.config.readBuffer) !== null && _a !== void 0 ? _a : 4, handleUnprocessed));
    }
    putItems(items) {
        const chunks = [];
        let i = 0;
        const n = items.length;
        while (i < n) {
            chunks.push(items.slice(i, (i += this.config.writeBuffer || 25)));
        }
        return Promise.all(chunks.map((chunk) => this.config.client
            .batchWrite({
            RequestItems: {
                [this.config.table]: chunk.map((item) => ({
                    PutRequest: {
                        Item: item,
                    },
                })),
            },
        })
            .promise()
            .catch((e) => {
            console.error("Error: AWS Error,", e);
            this.unprocessedItems.push(...chunk);
        })
            .then((results) => {
            var _a;
            if (results && results.UnprocessedItems && (((_a = results.UnprocessedItems[this.config.table]) === null || _a === void 0 ? void 0 : _a.length) || 0) > 0) {
                this.unprocessedItems.push(...results.UnprocessedItems[this.config.table].map((ui) => { var _a; return (_a = ui.PutRequest) === null || _a === void 0 ? void 0 : _a.Item; }));
            }
            return results;
        })));
    }
    put(item, condition) {
        if (!this.config.table) {
            throw new Error("Table not set");
        }
        const request = {
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
            console.error("Error: AWS Error,", e);
            this.unprocessedItems.push(item);
        })
            .then(() => this);
    }
    putIfNotExists(item) {
        const pkCondition = {
            operator: "attribute_not_exists",
            property: pkName(this.config.tableKeys),
        };
        return this.put(item, pkCondition);
    }
    update(pk, sk, attributes, condition, returnType) {
        if (!this.config.tableKeys) {
            throw new Error("Table keys not set");
        }
        if (!sk && this.config.tableKeys.sk) {
            throw new Error("Sort Key Required");
        }
        // TODO: Cleanup and extact
        const expression = Object.keys(attributes)
            .map((k) => `#${k.replace(/#\.:/g, "")} = :${k.replace(/#\./g, "")}`)
            .join(", ");
        const expressionNames = Object.keys(attributes).reduce((acc, curr) => ({ ...acc, ["#" + curr.replace(/#/g, "")]: curr }), {});
        const expressionValues = Object.entries(attributes).reduce((acc, curr) => ({
            ...acc,
            [":" + curr[0].replace(/\.:#/g, "")]: curr[1],
        }), {});
        const request = {
            TableName: this.config.table,
            Key: formatKeys(this.config.tableKeys, { pk, sk }),
            UpdateExpression: `SET ${expression}`,
            ...(Object.keys(expressionNames).length > 0 && {
                ExpressionAttributeNames: expressionNames,
            }),
            ...(Object.keys(expressionValues).length > 0 && {
                ExpressionAttributeValues: expressionValues,
            }),
            ...(returnType && { ReturnValues: returnType }),
        };
        if (condition) {
            const compiledCondition = conditionToDynamo(condition, {
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
            var _a;
            console.error("Error: AWS Error,", e);
            this.unprocessedItems.push({
                pk,
                ...(sk && ((_a = this.config.tableKeys) === null || _a === void 0 ? void 0 : _a.sk) && { sk }),
                attributes,
            });
        })
            .then((d) => {
            return d && "Attributes" in d && d.Attributes ? d.Attributes : null;
        });
    }
    delete(pk, sk, condition, returnType) {
        if (!this.config.tableKeys) {
            throw new Error("Table keys not set");
        }
        if (!sk && this.config.tableKeys.sk) {
            throw new Error("Table sort key not set");
        }
        const request = {
            TableName: this.config.table,
            Key: formatKeys(this.config.tableKeys, { pk, sk }),
            ...(returnType && { ReturnValues: returnType }),
        };
        if (condition) {
            const compiledCondition = conditionToDynamo(condition);
            request.ConditionExpression = compiledCondition.Condition;
            request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
            request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
        }
        else {
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
            console.error("Error: AWS Error,", e);
            this.unprocessedItems.push({ pk, sk });
        })
            .then((old) => (old && "Attributes" in old && old.Attributes ? old.Attributes : null));
    }
    handleUnprocessed(callback) {
        this.unprocessedItems.map(callback);
        return this;
    }
}
exports.Pipeline = Pipeline;
const formatKeys = (keys, item) => ({
    [pkName(keys)]: item.pk,
    ...(keys.sk && {
        [keys.sk]: item.sk,
    }),
});
const pkName = (keys) => keys.pk;
// const skName = (keys: KeySet) => keys.sk;
// const validatePk = (keys: KeySet, item: any) =>
// pkName(keys) in item && typeof item[pkName(keys)] === (typeof keys.pk === "string" ? "string" : keys.pk.type);
function operatorToSymbol(operator) {
    switch (operator) {
        case "begins_with":
            return `begins_with(#p1, :v1)`;
        case "between":
            return `#p1 BETWEEN :v1 AND :v2`;
        case ">":
        case "<":
        case ">=":
        case "<=":
        case "=":
            return `#p1 ${operator} :v1`;
        default:
            throw new Error("Operator not found: " + operator);
    }
}
function conditionToDynamo(condition, mergeCondition) {
    const result = mergeCondition ||
        {
            Condition: "",
        };
    if (!condition) {
        return result;
    }
    if ("logical" in condition) {
        const logicalLhs = conditionToDynamo(condition.lhs, result);
        const logicalRhs = conditionToDynamo(condition.rhs, {
            ...result,
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
        result.Condition = `${logicalLhs.Condition + (logicalLhs.Condition.length ? " " : "")}${condition.logical} ${logicalRhs.Condition}`;
        Object.entries({
            ...logicalRhs.ExpressionAttributeNames,
            ...logicalLhs.ExpressionAttributeNames,
        }).forEach(([name, value]) => {
            if (!result.ExpressionAttributeNames) {
                result.ExpressionAttributeNames = {};
            }
            result.ExpressionAttributeNames[name] = value;
        });
        Object.entries({
            ...logicalRhs.ExpressionAttributeValues,
            ...logicalLhs.ExpressionAttributeValues,
        }).forEach(([name, value]) => {
            if (!result.ExpressionAttributeValues) {
                result.ExpressionAttributeValues = {};
            }
            result.ExpressionAttributeValues[name] = value;
        });
        return result;
    }
    const names = conditionToAttributeNames(condition, result.ExpressionAttributeNames ? Object.keys(result.ExpressionAttributeNames).length : 0);
    const values = conditionToAttributeValues(condition, result.ExpressionAttributeValues ? Object.keys(result.ExpressionAttributeValues).length : 0);
    const conditionString = conditionToConditionString(condition, result.ExpressionAttributeNames ? Object.keys(result.ExpressionAttributeNames).length : 0, result.ExpressionAttributeValues ? Object.keys(result.ExpressionAttributeValues).length : 0);
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
function comparisonOperator(condition, nameStart, valueStart) {
    const lhs = typeof condition.lhs === "string" ? "#p" + nameStart : "#p" + nameStart;
    (typeof condition.lhs === "string" || "property" in condition.lhs) && (nameStart += 1);
    const rhs = "property" in condition.rhs ? "#p" + nameStart : ":v" + valueStart;
    return `${typeof condition.lhs !== "string" && "function" in condition.lhs ? condition.lhs.function + "(" : ""}${lhs}${typeof condition.lhs !== "string" && "function" in condition.lhs ? ")" : ""} ${condition.operator} ${"function" in condition.rhs ? condition.rhs.function + "(" : ""}${rhs}${"function" in condition.rhs ? ")" : ""}`;
}
function conditionToConditionString(condition, nameCountStart, valueCountStart) {
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
            return comparisonOperator(condition, nameStart, valueStart);
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
function conditionToAttributeValues(condition, countStart = 0) {
    const values = {};
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
function setPropertyValue(value, values, countStart) {
    // note this is the main place to change if we switch from document client to the regular dynamodb client
    const dynamoValue = Array.isArray(value)
        ? value.join("")
        : typeof value === "boolean" || typeof value === "string" || typeof value === "number"
            ? value
            : value === null
                ? true
                : (value === null || value === void 0 ? void 0 : value.toString()) || true;
    return setRawPropertyValue(dynamoValue, values, countStart);
}
function setRawPropertyValue(value, values, countStart) {
    const name = ":v" + (Object.keys(values).length + countStart);
    values[name] = value;
    return values;
}
function conditionToAttributeNames(condition, countStart = 0) {
    const names = {};
    if ("lhs" in condition && condition.lhs && (typeof condition.lhs === "string" || "property" in condition.lhs)) {
        splitAndSetPropertyName(typeof condition.lhs === "string" ? condition.lhs : condition.lhs.property, names, countStart);
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
function splitAndSetPropertyName(propertyName, names, countStart) {
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
