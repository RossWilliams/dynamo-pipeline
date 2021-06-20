"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ScanQueryPipeline = exports.sortKey = void 0;
const dynamodb_1 = require("aws-sdk/clients/dynamodb");
const helpers_1 = require("./helpers");
const QueryFetcher_1 = require("./QueryFetcher");
const TableIterator_1 = require("./TableIterator");
const TokenBucket_1 = require("./TokenBucket");
const sortKey = (...args) => {
    if (args.length === 3) {
        return ["between", "and", args[1], args[2]];
    }
    return args;
};
exports.sortKey = sortKey;
class ScanQueryPipeline {
    constructor(tableName, keys, index, config) {
        this.sortKey = exports.sortKey;
        // 'as unknown' is a shortcut to use KD, otherwise type definitions throughout the class are too long
        this.config = this.createConfig(tableName, index, keys, config);
        if (config === null || config === void 0 ? void 0 : config.readCapacityUnitLimit) {
            this.readTokenBucket = new TokenBucket_1.TokenBucket(index || tableName, config.readCapacityUnitLimit);
        }
        return this;
    }
    createConfig(tableName, index, keys, config) {
        return {
            table: tableName,
            readBuffer: 1,
            readBatchSize: 100,
            ...config,
            keys,
            index: index,
            client: (config && config.client) || new dynamodb_1.DocumentClient(),
        };
    }
    /**
     * A convenience long-form method to set the read buffer.
     * @param readBuffer The read buffer from the constructor configuration options
     * @returns this
     */
    withReadBuffer(readBuffer) {
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
    withReadBatchSize(readBatchSize) {
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
    query(keyConditions, options) {
        const request = this.buildQueryScanRequest({ ...options, keyConditions });
        const fetchOptions = {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            tokenBucket: this.readTokenBucket,
            ...options,
        };
        return new TableIterator_1.TableIterator(new QueryFetcher_1.QueryFetcher(request, this.config.client, "query", fetchOptions), this);
    }
    /**
     * Prepares a dynamodb scan to be executed by the returned TableIterator.
     * @param options Scan specific options and Pipeline configuration override options
     * @returns TableIterator
     */
    scan(options) {
        const request = this.buildQueryScanRequest(options !== null && options !== void 0 ? options : {});
        const fetchOptions = {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            ...options,
        };
        return new TableIterator_1.TableIterator(new QueryFetcher_1.QueryFetcher(request, this.config.client, "scan", fetchOptions), this);
    }
    buildQueryScanRequest(options) {
        const pkName = this.config.keys.pk;
        const skName = this.config.keys.sk;
        const skValue = options.keyConditions && typeof skName !== "undefined" && options.keyConditions && skName in options.keyConditions
            ? options.keyConditions[skName]
            : null;
        const request = {
            TableName: this.config.table,
            ...(options.limit && {
                Limit: options.limit,
            }),
            ...(this.config.index && { IndexName: this.config.index }),
            ...(options.keyConditions && {
                KeyConditionExpression: `#p0 = :v0` + (skValue ? ` AND ${helpers_1.skQueryToDynamoString(skValue)}` : ""),
            }),
            ConsistentRead: Boolean(options.consistentRead),
            ScanIndexForward: Boolean(!options.sortDescending),
        };
        const [skVal1, skVal2] = (skValue === null || skValue === void 0 ? void 0 : skValue.length) === 4 ? [skValue[2], skValue[3]] : (skValue === null || skValue === void 0 ? void 0 : skValue.length) === 2 ? [skValue[1], null] : [null, null];
        const keySubstitues = {
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
            const compiledCondition = helpers_1.conditionToDynamo(options.filters, keySubstitues);
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
}
exports.ScanQueryPipeline = ScanQueryPipeline;
ScanQueryPipeline.sortKey = exports.sortKey;
