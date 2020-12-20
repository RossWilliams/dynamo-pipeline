"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BatchGetFetcher = void 0;
const AbstractFetcher_1 = require("./AbstractFetcher");
class BatchGetFetcher extends AbstractFetcher_1.BatchFetcher {
    constructor(client, operation, items, batchSize = 100, bufferCapacity = 4, onUnprocessedKeys) {
        super(client, bufferCapacity, batchSize);
        this.retryKeys = [];
        this.operation = operation;
        this.onUnprocessedKeys = onUnprocessedKeys;
        if (operation === "batchGet") {
            const chunks = [];
            items.forEach((item) => {
                let i = 0;
                const n = item.keyItems.length;
                while (i < n) {
                    chunks.push({ tableName: item.tableName, keyItems: item.keyItems.slice(i, (i += batchSize)) });
                }
            });
            this.chunks = chunks;
        }
        else {
            // TODO: transactions don't support chunking yet.
            this.chunks = items;
        }
        this.nextToken = 0;
    }
    retry() {
        this.chunks = this.retryKeys || [];
        this.nextToken = 0;
        this.retryKeys = null;
        return this.fetchNext();
    }
    fetchStrategy() {
        if (this.retryKeys && this.retryKeys.length && this.nextToken === null && !this.isActive()) {
            // if finished fetching initial requests, begine to process the retry keys
            return this.retry();
        }
        else if (this.bufferSize >= this.bufferCapacity ||
            this.chunks.length <= this.nextToken ||
            this.nextToken === null) {
            // return the current promise if buffer at capacity, or if there are no more items to fetch
            return this.activeRequests[0] || null;
        }
        let promise = null;
        if (this.operation === "transactGet") {
            promise = this.documentClient
                .transactGet(this.createTransactionRequest(this.chunks[this.nextToken]))
                .promise();
        }
        else if (this.operation === "batchGet") {
            promise = this.documentClient
                .batchGet(this.createBatchGetRequest(this.chunks[this.nextToken]))
                .promise();
        }
        this.nextToken = typeof this.chunks[this.nextToken + 1] !== "undefined" ? this.nextToken + 1 : null;
        return this.fetchNext(promise);
    }
    processResult(data) {
        let responseItems = [];
        if (data && data.Responses && Array.isArray(data.Responses)) {
            // transaction
            responseItems = data.Responses.map((r) => r.Item).filter(notEmpty);
        }
        else if (data && data.Responses && !Array.isArray(data.Responses)) {
            // batch, flatten each table response
            responseItems = [].concat(...Object.values(data.Responses)).filter(notEmpty);
        }
        if (data) {
            const unprocessedKeys = "UnprocessedKeys" in data && data.UnprocessedKeys;
            if (unprocessedKeys && Array.isArray(this.retryKeys)) {
                const retryItems = Object.entries(unprocessedKeys).map(([tableName, keys]) => splitInHalf(keys.Keys)
                    .filter(notEmpty)
                    .map((k) => ({
                    tableName,
                    keyItems: k,
                })));
                this.retryKeys.push(...[].concat(...retryItems));
            }
            else if (unprocessedKeys && typeof this.onUnprocessedKeys !== "undefined") {
                const iter = this.onUnprocessedKeys.bind(this);
                Object.entries(unprocessedKeys).forEach(([_tableName, keys]) => iter(keys.Keys));
            }
        }
        this.totalReturned += responseItems.length;
        this.results.push(...responseItems);
    }
    isDone() {
        return super.isDone() && (!this.retryKeys || this.retryKeys.length === 0);
    }
    createTransactionRequest(items) {
        return {
            TransactItems: items.map((item) => ({
                Get: {
                    Key: item.keys,
                    TableName: item.tableName,
                },
            })),
        };
    }
    // each batch handles a single table for now...
    createBatchGetRequest(items) {
        // when multiple tables are supported in a single batch
        // switch to items.reduce(acc, curr) => ({...acc, [curr.tableName]: curr.keyItems,}),{})
        const request = {
            RequestItems: {
                [items.tableName]: {
                    Keys: items.keyItems,
                },
            },
        };
        return request;
    }
}
exports.BatchGetFetcher = BatchGetFetcher;
function notEmpty(val) {
    return !!val;
}
function splitInHalf(arr) {
    return [arr.slice(0, Math.ceil(arr.length / 2)), arr.slice(Math.ceil(arr.length / 2), arr.length)];
}
