import { DeleteCommand, PutCommand, UpdateCommand, } from "@aws-sdk/lib-dynamodb";
import { BatchGetFetcher } from "./BatchFetcher";
import { TableIterator } from "./TableIterator";
import { BatchWriter } from "./BatchWriter";
import { conditionToDynamo, pkName } from "./helpers";
import { ScanQueryPipeline } from "./ScanQueryPipeline";
export class Pipeline extends ScanQueryPipeline {
    constructor(tableName, keys, config) {
        super(tableName, keys, undefined, config);
        return this;
    }
    withWriteBuffer(writeBuffer = 30) {
        if (writeBuffer < 0) {
            throw new Error("Write buffer out of range");
        }
        this.config.writeBuffer = writeBuffer;
        return this;
    }
    withWriteBatchSize(writeBatchSize = 25) {
        if (writeBatchSize < 1 || writeBatchSize > 25) {
            throw new Error("Write batch size out of range");
        }
        this.config.writeBatchSize = writeBatchSize;
        return this;
    }
    createIndex(name, definition) {
        const { keys, ...config } = this.config;
        return new ScanQueryPipeline(this.config.table, definition, name, config);
    }
    transactGet(keys, options) {
        // get keys into a standard format, filter out any non-key attributes
        const transactGetItems = typeof keys[0] !== "undefined" && !("tableName" in keys[0]) && !("keys" in keys[0])
            ? keys.map((k) => ({
                tableName: this.config.table,
                keys: this.keyAttributesOnly(k, this.config.keys),
            }))
            : keys.map((key) => ({
                tableName: key.tableName,
                keys: this.keyAttributesOnly(key.keys, key.keyDefinition),
            }));
        return new TableIterator(new BatchGetFetcher(this.config.client, "transactGet", transactGetItems, {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            ...options,
        }), this);
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
        const tableKeys = this.keyAttributesOnlyFromArray(keys, this.config.keys);
        const batchGetItems = { tableName: this.config.table, keys: tableKeys };
        return new TableIterator(new BatchGetFetcher(this.config.client, "batchGet", batchGetItems, {
            batchSize: this.config.readBatchSize,
            bufferCapacity: this.config.readBuffer,
            onUnprocessedKeys: handleUnprocessed,
            ...options,
        }), this);
    }
    async putItems(items, options) {
        const handleUnprocessed = (keys) => {
            this.unprocessedItems.push(...keys);
        };
        if (typeof (options === null || options === void 0 ? void 0 : options.bufferCapacity) === "number" && options.bufferCapacity < 0) {
            throw new Error("Buffer capacity is out of range");
        }
        if (typeof (options === null || options === void 0 ? void 0 : options.batchSize) === "number" && (options.batchSize < 1 || options.batchSize > 25)) {
            throw new Error("Batch size is out of range");
        }
        const writer = new BatchWriter(this.config.client, { tableName: this.config.table, records: items }, {
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
        const promise = this.config.client.send(new PutCommand(request));
        return promise
            .catch((e) => {
            console.error("Error: AWS Error, Put,", e);
            this.unprocessedItems.push(item);
        })
            .then(() => this);
    }
    putIfNotExists(item) {
        const pkCondition = {
            operator: "attribute_not_exists",
            property: pkName(this.config.keys),
        };
        return this.put(item, pkCondition);
    }
    buildUpdateRequest(key, attributes, options) {
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
            Key: this.keyAttributesOnly(key, this.config.keys),
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
        return request;
    }
    update(key, attributes, options) {
        // TODO: Cleanup and extact
        const request = this.buildUpdateRequest(key, attributes, options);
        return this.config.client
            .send(new UpdateCommand(request))
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
            Key: this.keyAttributesOnly(key, this.config.keys),
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
                property: pkName(this.config.keys),
            });
            request.ConditionExpression = compiledCondition.Condition;
            request.ExpressionAttributeNames = compiledCondition.ExpressionAttributeNames;
            request.ExpressionAttributeValues = compiledCondition.ExpressionAttributeValues;
        }
        return this.config.client
            .send(new DeleteCommand(request))
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
    keyAttributesOnlyFromArray(items, keyDefinition) {
        return items.map((item) => this.keyAttributesOnly(item, keyDefinition));
    }
    keyAttributesOnly(item, keyDefinition) {
        return {
            [keyDefinition.pk]: item[keyDefinition.pk],
            ...(typeof this.config.keys.sk === "string" && {
                [this.config.keys.sk]: item[this.config.keys.sk],
            }),
        };
    }
}
