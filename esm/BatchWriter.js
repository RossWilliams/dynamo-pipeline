import { BatchWriteCommand, } from "@aws-sdk/lib-dynamodb";
export class BatchWriter {
    constructor(client, items, options) {
        this.activeRequests = [];
        this.retryKeys = [];
        this.errors = null;
        this.batchSize = 25;
        this.bufferCapacity = 3;
        this.backoffActive = false;
        this.client = client;
        this.tableName = items.tableName;
        this.batchSize = options.batchSize;
        this.bufferCapacity = options.bufferCapacity;
        this.onUnprocessedItems = options.onUnprocessedItems;
        this.chunks = this.chunkBatchWrites(items);
        this.nextToken = 0;
    }
    async execute() {
        do {
            if (this.errors) {
                return Promise.reject(this.errors);
            }
            if (!this.isDone()) {
                await this.writeChunk();
            }
        } while (!this.isDone());
        await Promise.all(this.activeRequests);
    }
    chunkBatchWrites(items) {
        const chunks = [];
        let i = 0;
        const n = items.records.length;
        while (i < n) {
            chunks.push(items.records.slice(i, (i += this.batchSize || 25)));
        }
        return chunks;
    }
    async writeChunk() {
        if (this.retryKeys && this.retryKeys.length && this.nextToken === null && !this.isActive()) {
            // if finished fetching initial requests, begin to process the retry keys
            return this.retry();
        }
        else if (this.activeRequests.length >= this.bufferCapacity || this.nextToken === null || this.backoffActive) {
            // return the current promise if buffer at capacity, or if there are no more items to fetch
            return this.activeRequests[0] || null;
        }
        else if (!this.hasNextChunk()) {
            this.nextToken = null;
            // let the caller wait until all active requests are finished
            return Promise.all(this.activeRequests).then();
        }
        const chunk = this.getNextChunk();
        if (chunk) {
            const command = new BatchWriteCommand({
                RequestItems: {
                    [this.tableName]: chunk.map((item) => ({
                        PutRequest: {
                            Item: item,
                        },
                    })),
                },
            });
            const promise = this.client
                .send(command)
                .catch((e) => {
                console.error("Error: AWS Error, Put Items", e);
                if (this.onUnprocessedItems) {
                    this.onUnprocessedItems(chunk);
                }
                this.errors = e;
            })
                .then((results) => {
                var _a, _b, _c, _d;
                if (results &&
                    results.$metadata &&
                    ((_b = (_a = results.$metadata) === null || _a === void 0 ? void 0 : _a.attempts) !== null && _b !== void 0 ? _b : 1) > 1 &&
                    ((_d = (_c = results.$metadata) === null || _c === void 0 ? void 0 : _c.totalRetryDelay) !== null && _d !== void 0 ? _d : 0) > 0) {
                    // reduce buffer capacity when request required multiple attempts due to backoff
                    this.bufferCapacity = Math.max(Math.floor((this.bufferCapacity * 3) / 4), 5);
                    this.backoffActive = true;
                }
                this.processResult(results, promise);
            });
            this.activeRequests.push(promise);
        }
    }
    getNextChunk() {
        if (this.nextToken === null) {
            /* istanbul ignore next */
            return null;
        }
        const chunk = this.chunks[this.nextToken] || null;
        this.nextToken += 1;
        return chunk;
    }
    isActive() {
        return this.activeRequests.length > 0;
    }
    processResult(data, request) {
        var _a;
        this.activeRequests = this.activeRequests.filter((r) => r !== request);
        if (!this.activeRequests.length || !data || !data.UnprocessedItems) {
            this.backoffActive = false;
        }
        if (data && data.UnprocessedItems && (((_a = data.UnprocessedItems[this.tableName]) === null || _a === void 0 ? void 0 : _a.length) || 0) > 0) {
            // eslint-disable-next-line
            const unprocessedItems = data.UnprocessedItems[this.tableName].map((ui) => { var _a; return (_a = ui.PutRequest) === null || _a === void 0 ? void 0 : _a.Item; });
            if (Array.isArray(this.retryKeys)) {
                const retryItems = splitInHalf(unprocessedItems).filter(notEmpty);
                this.retryKeys.push(...retryItems);
            }
            else if (this.onUnprocessedItems) {
                this.onUnprocessedItems(unprocessedItems);
            }
        }
    }
    retry() {
        this.chunks = this.retryKeys || [];
        this.nextToken = 0;
        this.retryKeys = null;
        return this.writeChunk();
    }
    isDone() {
        return !this.isActive() && (!this.retryKeys || this.retryKeys.length === 0) && this.nextToken === null;
    }
    hasNextChunk() {
        if (this.nextToken === null || this.nextToken >= this.chunks.length) {
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
