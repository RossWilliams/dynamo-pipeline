import { BatchGetCommand, TransactGetCommand, } from "@aws-sdk/lib-dynamodb";
import { AbstractFetcher } from "./AbstractFetcher";
export class BatchGetFetcher extends AbstractFetcher {
    constructor(client, operation, items, options) {
        super(client, options);
        this.retryKeys = [];
        this.consistentRead = false;
        this.operation = operation;
        this.onUnprocessedKeys = options.onUnprocessedKeys;
        this.consistentRead = Boolean(options.consistentRead);
        if (operation === "batchGet" && !Array.isArray(items)) {
            this.chunks = this.chunkBatchRequests(items);
        }
        else {
            // Transactions don't support chunking, its a transaction
            this.chunks = [items];
        }
        this.nextToken = 0;
    }
    chunkBatchRequests(items) {
        const chunks = [];
        const n = items.keys.length;
        let i = 0;
        while (i < n) {
            chunks.push({ tableName: items.tableName, keys: items.keys.slice(i, (i += this.batchSize)) });
        }
        return chunks;
    }
    retry() {
        this.chunks = this.retryKeys || [];
        this.nextToken = 0;
        this.retryKeys = null;
        return this.fetchNext();
        // TODO: Batch Get needs to be tested with chunk size of 1 and three items
    }
    fetchStrategy() {
        if (this.retryKeys && this.retryKeys.length && this.nextToken === null && !this.isActive()) {
            // if finished fetching initial requests, begin to process the retry keys
            return this.retry();
        }
        else if (this.bufferSize >= this.bufferCapacity ||
            (typeof this.nextToken === "number" && this.chunks.length <= this.nextToken) ||
            this.nextToken === null) {
            // return the current promise if buffer at capacity, or if there are no more items to fetch
            return this.activeRequests[0] || null;
        }
        else if (!this.hasNextChunk()) {
            /* istanbul ignore next */
            return null;
        }
        let promise = null;
        if (this.operation === "transactGet") {
            const transactionRequest = this.createTransactionRequest();
            if (transactionRequest === null) {
                /* istanbul ignore next */
                return null;
            }
            promise = this.documentClient.send(new TransactGetCommand(transactionRequest));
        }
        else if (this.operation === "batchGet") {
            const batchGetRequest = this.createBatchGetRequest();
            if (batchGetRequest === null) {
                /* istanbul ignore next */
                return null;
            }
            promise = this.documentClient.send(new BatchGetCommand(batchGetRequest));
        }
        if (typeof this.nextToken === "number" && typeof this.chunks[this.nextToken + 1] !== "undefined") {
            this.nextToken = this.nextToken + 1;
        }
        else {
            this.nextToken = null;
        }
        return promise;
    }
    processResult(data) {
        let responseItems = [];
        if (data && data.Responses && Array.isArray(data.Responses)) {
            // transaction
            responseItems = data.Responses.map((r) => r.Item).filter(notEmpty);
        }
        else if (data && data.Responses && !Array.isArray(data.Responses)) {
            // batch, flatten each table response
            responseItems = []
                .concat(...Object.values(data.Responses))
                .filter(notEmpty);
        }
        if (data) {
            const unprocessedKeys = "UnprocessedKeys" in data && data.UnprocessedKeys;
            if (unprocessedKeys) {
                Object.entries(unprocessedKeys).forEach(([tableName, keys]) => {
                    this.processError({ tableName, errorKeys: keys.Keys });
                });
            }
        }
        this.totalReturned += responseItems.length;
        this.results.push(...responseItems);
    }
    processError(err) {
        if (err && "tableName" in err && Array.isArray(this.retryKeys)) {
            const retryItems = splitInHalf(err.errorKeys)
                .filter(notEmpty)
                .map((k) => ({
                tableName: err.tableName,
                keys: k,
            }));
            this.retryKeys.push(...[].concat(...retryItems));
        }
        else if (err && "errorKeys" in err && typeof this.onUnprocessedKeys !== "undefined") {
            this.onUnprocessedKeys(err.errorKeys);
        }
    }
    isDone() {
        return super.isDone() && (!this.retryKeys || this.retryKeys.length === 0);
    }
    createTransactionRequest() {
        const currentChunk = typeof this.nextToken === "number"
            ? this.chunks[this.nextToken]
            : undefined;
        if (!currentChunk) {
            /* istanbul ignore next */
            return null;
        }
        const transaction = {
            TransactItems: currentChunk.map((item) => ({
                Get: {
                    Key: item.keys,
                    TableName: item.tableName,
                },
            })),
        };
        return transaction;
    }
    // each batch handles a single table for now...
    createBatchGetRequest() {
        const currentChunk = typeof this.nextToken === "number" ? this.chunks[this.nextToken] : undefined;
        if (!currentChunk) {
            /* istanbul ignore next */
            return null;
        }
        // when multiple tables are supported in a single batch
        // switch to items.reduce(acc, curr) => ({...acc, [curr.tableName]: curr.keyItems,}),{})
        const request = {
            RequestItems: {
                [currentChunk.tableName]: {
                    ConsistentRead: this.consistentRead,
                    Keys: currentChunk.keys,
                },
            },
        };
        return request;
    }
    hasNextChunk() {
        if (typeof this.nextToken !== "number" || this.nextToken >= this.chunks.length) {
            return false;
        }
        return true;
    }
}
function notEmpty(val) {
    if (Array.isArray(val) && !val.length) {
        return false;
    }
    return !!val;
}
function splitInHalf(arr) {
    return [arr.slice(0, Math.ceil(arr.length / 2)), arr.slice(Math.ceil(arr.length / 2), arr.length)];
}
