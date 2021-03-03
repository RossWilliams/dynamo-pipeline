import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { conditionToDynamo, skQueryToDynamoString } from "./helpers";
import { QueryFetcher } from "./QueryFetcher";
import { TableIterator } from "./TableIterator";
export const sortKey = (...args) => {
    if (args.length === 3) {
        return ["between", "and", args[1], args[2]];
    }
    return args;
};
export class ScanQueryPipeline {
    constructor(tableName, keys, index, config) {
        this.config = {
            table: tableName,
            readBuffer: 1,
            writeBuffer: 3,
            readBatchSize: 100,
            writeBatchSize: 25,
            ...config,
            // shortcut to use KD, otherwise type definitions throughout the
            // class are too long
            keys: keys,
            index: index,
            client: (config && config.client) || new DocumentClient(),
        };
        this.unprocessedItems = [];
        return this;
    }
    withReadBuffer(readBuffer) {
        if (readBuffer < 0) {
            throw new Error("Read buffer out of range");
        }
        this.config.readBuffer = readBuffer;
        return this;
    }
    withReadBatchSize(readBatchSize) {
        if (readBatchSize < 1) {
            throw new Error("Read batch size out of range");
        }
        this.config.readBatchSize = readBatchSize;
        return this;
    }
    query(keyConditions, options) {
        const request = this.buildQueryScanRequest({ ...options, keyConditions });
        const fetchOptions = {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            ...options,
        };
        return new TableIterator(new QueryFetcher(request, this.config.client, "query", fetchOptions), this);
    }
    scan(options) {
        const request = this.buildQueryScanRequest(options !== null && options !== void 0 ? options : {});
        const fetchOptions = {
            bufferCapacity: this.config.readBuffer,
            batchSize: this.config.readBatchSize,
            ...options,
        };
        return new TableIterator(new QueryFetcher(request, this.config.client, "scan", fetchOptions), this);
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
                KeyConditionExpression: `#p0 = :v0` + (skValue ? ` AND #p1 ${skQueryToDynamoString(skValue)}` : ""),
            }),
            ConsistentRead: Boolean(options.consistentRead),
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
}
