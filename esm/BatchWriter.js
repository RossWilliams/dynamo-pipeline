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
            const request = this.client.batchWrite({
                RequestItems: {
                    [this.tableName]: chunk.map((item) => ({
                        PutRequest: {
                            Item: item,
                        },
                    })),
                },
            });
            if (request && typeof request.on === "function") {
                request.on("retry", (e) => {
                    var _a;
                    if ((_a = e === null || e === void 0 ? void 0 : e.error) === null || _a === void 0 ? void 0 : _a.retryable) {
                        // reduce buffer capacity on retryable error
                        this.bufferCapacity = Math.max(Math.floor((this.bufferCapacity * 3) / 4), 5);
                        this.backoffActive = true;
                    }
                });
            }
            const promise = request
                .promise()
                .catch((e) => {
                console.error("Error: AWS Error, Put Items", e);
                if (this.onUnprocessedItems) {
                    this.onUnprocessedItems(chunk);
                }
                this.errors = e;
            })
                .then((results) => {
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
