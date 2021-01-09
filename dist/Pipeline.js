"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Pipeline = void 0;
const dynamodb_1 = require("aws-sdk/clients/dynamodb");
const BatchFetcher_1 = require("./BatchFetcher");
const TableIterator_1 = require("./TableIterator");
const QueryFetcher_1 = require("./QueryFetcher");
const BatchWriter_1 = require("./BatchWriter");
class Pipeline {
    constructor(tableName, tableKeys, config) {
        this.config = {
            table: tableName,
            indexes: {},
            readBuffer: 1,
            writeBuffer: 30,
            readBatchSize: 100,
            writeBatchSize: 25,
            // shortcut to use KD, otherwise type definitions throughout the
            // class are too long
            tableKeys: tableKeys,
            client: (config && config.client) || new dynamodb_1.DocumentClient(),
            ...config,
        };
        this.unprocessedItems = [];
        return this;
    }
    withKeys(tableKeys) {
        return new Pipeline(this.config.table, tableKeys, {
            ...this.config,
        });
    }
    withIndex(name, keyDefinition) {
        this.config.indexes[name] = { ...keyDefinition };
        return this;
    }
    withReadBuffer(readBuffer = 1) {
        if (readBuffer < 0) {
            throw new Error("Read buffer out of range");
        }
        this.config.readBuffer = readBuffer;
        return this;
    }
    withWriteBuffer(writeBuffer = 30) {
        if (writeBuffer < 0) {
            throw new Error("Write buffer out of range");
        }
        this.config.writeBuffer = writeBuffer;
        return this;
    }
    withReadBatchSize(readBatchSize = 25) {
        if (readBatchSize < 1) {
            throw new Error("Read batch size out of range");
        }
        this.config.readBatchSize = readBatchSize;
        return this;
    }
    withWriteBatchSize(writeBatchSize = 25) {
        if (writeBatchSize < 1 || writeBatchSize > 25) {
            throw new Error("Write batch size out of range");
        }
        this.config.writeBatchSize = writeBatchSize;
        return this;
    }
    queryIndex(indexName, KeyConditions, options) {
        return this.query(KeyConditions, { ...options, indexName });
    }
    query(keyConditions, options) {
        const request = this.buildQueryScanRequest({ ...options, keyConditions });
        const fetchOptions = {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            ...options,
        };
        return new TableIterator_1.TableIterator(this, new QueryFetcher_1.QueryFetcher(request, this.config.client, "query", fetchOptions));
    }
    scanIndex(indexName, options) {
        return this.scan({ ...options, indexName });
    }
    scan(options) {
        const request = this.buildQueryScanRequest(options !== null && options !== void 0 ? options : {});
        const fetchOptions = {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            ...options,
        };
        return new TableIterator_1.TableIterator(this, new QueryFetcher_1.QueryFetcher(request, this.config.client, "scan", fetchOptions));
    }
    transactGet(keys, options) {
        // get keys into a standard format, filter out any non-key attributes
        const transactGetItems = typeof keys[0] !== "undefined" && !("tableName" in keys[0]) && !("keys" in keys[0])
            ? keys.map((k) => ({
                tableName: this.config.table,
                keys: this.keyAttributesOnly(k, this.config.tableKeys),
            }))
            : keys.map((key) => ({
                tableName: key.tableName,
                keys: this.keyAttributesOnly(key.keys, key.keyDefinition),
            }));
        return new TableIterator_1.TableIterator(this, new BatchFetcher_1.BatchGetFetcher(this.config.client, "transactGet", transactGetItems, {
            bufferCapacity: this.config.readBuffer,
            ...options,
        }));
    }
    getItems(keys, options) {
        const handleUnprocessed = (keys) => {
            this.unprocessedItems.push(...keys);
        };
        if (typeof (options === null || options === void 0 ? void 0 : options.batchSize) === "number" && (options.batchSize > 100 || options.batchSize < 1)) {
            throw new Error("Batch size out of range");
        }
        if (typeof (options === null || options === void 0 ? void 0 : options.bufferCapacity) === "number" && options.bufferCapacity < 0) {
            throw new Error("Buffer capacity is out of range");
        }
        // filter out any non-key attributes
        const tableKeys = this.keyAttributesOnlyFromArray(keys, this.config.tableKeys);
        const batchGetItems = { tableName: this.config.table, keys: tableKeys };
        return new TableIterator_1.TableIterator(this, new BatchFetcher_1.BatchGetFetcher(this.config.client, "batchGet", batchGetItems, {
            batchSize: this.config.readBatchSize,
            bufferCapacity: this.config.readBuffer,
            onUnprocessedKeys: handleUnprocessed,
            ...options,
        }));
    }
    async putItems(items, options) {
        const handleUnprocessed = (keys) => {
            this.unprocessedItems.push(...keys);
        };
        if (typeof (options === null || options === void 0 ? void 0 : options.bufferCapacity) === "number" && options.bufferCapacity < 0) {
            throw new Error("Buffer capacity is out of range");
        }
        if (typeof (options === null || options === void 0 ? void 0 : options.batchSize) === "number" && options.batchSize < 1) {
            throw new Error("Batch size is out of range");
        }
        const writer = new BatchWriter_1.BatchWriter(this.config.client, { tableName: this.config.table, records: items }, {
            batchSize: this.config.writeBatchSize,
            bufferCapacity: this.config.writeBuffer,
            onUnprocessedItems: handleUnprocessed,
            ...options,
        });
        await writer.execute();
        return this;
    }
    put(item, condition) {
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
            console.error("Error: AWS Error, Put,", e);
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
    update(key, attributes, options) {
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
            Key: this.keyAttributesOnly(key, this.config.tableKeys),
            UpdateExpression: `SET ${expression}`,
            ...(Object.keys(expressionNames).length > 0 && {
                ExpressionAttributeNames: expressionNames,
            }),
            ...(Object.keys(expressionValues).length > 0 && {
                ExpressionAttributeValues: expressionValues,
            }),
            ...((options === null || options === void 0 ? void 0 : options.returnType) && { ReturnValues: options.returnType }),
        };
        if (options === null || options === void 0 ? void 0 : options.condition) {
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
            .then((d) => {
            return d && "Attributes" in d && d.Attributes ? d.Attributes : null;
        });
    }
    delete(key, options) {
        const request = {
            TableName: this.config.table,
            Key: this.keyAttributesOnly(key, this.config.tableKeys),
            ...((options === null || options === void 0 ? void 0 : options.returnType) && { ReturnValues: options.returnType }),
        };
        if (options === null || options === void 0 ? void 0 : options.condition) {
            const compiledCondition = conditionToDynamo(options.condition);
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
            if (options === null || options === void 0 ? void 0 : options.reportError) {
                console.error("Error: AWS Error, Delete", e, request);
                this.unprocessedItems.push(key);
            }
        })
            .then((old) => (old && "Attributes" in old && old.Attributes ? old.Attributes : null));
    }
    handleUnprocessed(callback) {
        this.unprocessedItems.map(callback);
        return this;
    }
    buildQueryScanRequest(options) {
        var _a, _b;
        let index;
        if (options.indexName) {
            const indexInConfig = this.config.indexes[options.indexName];
            if (indexInConfig) {
                index = { ...indexInConfig, name: options.indexName };
            }
        }
        if (options.indexName && typeof index === "undefined") {
            throw new Error(`Specified index not configured. You must specify index keys before accessing index ${options.indexName}`);
        }
        const request = {
            TableName: this.config.table,
            ...(options.limit && {
                Limit: options.limit,
            }),
            ...(index && { IndexName: index.name }),
            ...(options.keyConditions && {
                KeyConditionExpression: `#p0 = :v0` +
                    ("sk" in options.keyConditions && options.keyConditions.sk
                        ? ` AND #p1 ${skQueryToDynamoString(options.keyConditions.sk)}`
                        : ""),
            }),
        };
        const [_1, skVal1, _2, skVal2] = ((_a = options.keyConditions) === null || _a === void 0 ? void 0 : _a.sk)
            ? splitSkQueryToParts((_b = options.keyConditions) === null || _b === void 0 ? void 0 : _b.sk)
            : [undefined, undefined, undefined, undefined];
        const keySubstitues = {
            Condition: "",
            ExpressionAttributeNames: options.keyConditions
                ? {
                    "#p0": index ? index.pk : this.config.tableKeys.pk,
                    ...(options.keyConditions.sk && {
                        "#p1": index ? index.sk : this.config.tableKeys.sk,
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
        }
        else {
            request.ExpressionAttributeNames = keySubstitues.ExpressionAttributeNames;
            request.ExpressionAttributeValues = keySubstitues.ExpressionAttributeValues;
        }
        return request;
    }
    keyAttributesOnlyFromArray(items, keyDefinition) {
        return items.map((item) => this.keyAttributesOnly(item, keyDefinition));
    }
    keyAttributesOnly(item, keyDefinition) {
        return {
            [keyDefinition.pk]: item[keyDefinition.pk],
            ...(typeof this.config.tableKeys.sk === "string" && {
                [this.config.tableKeys.sk]: item[this.config.tableKeys.sk],
            }),
        };
    }
}
exports.Pipeline = Pipeline;
const pkName = (keys) => keys.pk;
function skQueryToDynamoString(operatorString) {
    const [operator, _value1, and, value2] = splitSkQueryToParts(operatorString);
    return `${operator} :v1 ${and ? "AND" : ""} ${value2 ? ":v2" : ""}`;
}
function splitSkQueryToParts(operatorString) {
    return operatorString.split(" ");
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
        result.Condition = `${logicalLhs.Condition + (logicalLhs.Condition.length ? " " : "")}${condition.logical} ${logicalRhs.Condition}`;
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
        Object.entries({
            ...logicalRhs.ExpressionAttributeValues,
            ...logicalLhs.ExpressionAttributeValues,
        }).forEach(([name, value]) => {
            if (!result.ExpressionAttributeValues) {
                result.ExpressionAttributeValues = {};
            }
            // @ts-expect-error:  Object.entries hard codes string as the key type
            // and indexing by template strings is invalid in ts 4.2.0
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
