"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AbstractFetcher = void 0;
class AbstractFetcher {
    constructor(client, options) {
        this.activeRequests = [];
        this.bufferSize = 0;
        this.bufferCapacity = 1;
        this.totalReturned = 0;
        this.results = [];
        this.errors = null;
        this.documentClient = client;
        this.tokenBucket = options.tokenBucket;
        this.bufferCapacity = options.bufferCapacity;
        this.batchSize = options.batchSize;
        this.limit = options.limit;
        this.nextToken = null;
    }
    // take in a promise to allow recursive calls,
    // batch fetcher can immediately create many requests
    fetchNext() {
        var _a;
        const { tokenBucket } = this;
        const tokensAvailable = (_a = tokenBucket === null || tokenBucket === void 0 ? void 0 : tokenBucket.peak()) !== null && _a !== void 0 ? _a : 0;
        if (typeof tokenBucket !== "undefined" && tokensAvailable < 0) {
            const waitTimeMs = Math.floor((Math.abs(tokensAvailable) / tokenBucket.fillRatePerSecond) * 1000);
            return new Promise((resolve, reject) => setTimeout(() => {
                resolve();
            }, waitTimeMs));
        }
        const fetchResponse = this.fetchStrategy();
        if (fetchResponse instanceof Promise && !this.activeRequests.includes(fetchResponse)) {
            return this.setupFetchProcessor(fetchResponse);
        }
        return fetchResponse;
    }
    removeCapacityFromTokenBucket(data) {
        var _a;
        if (typeof data === "object" && (data === null || data === void 0 ? void 0 : data.ConsumedCapacity)) {
            // Batch get or Transact Get
            if (Array.isArray(data.ConsumedCapacity)) {
                // we only track the table associated with the pipeline
                data.ConsumedCapacity.filter((cc) => { var _a; return cc.TableName === ((_a = this.tokenBucket) === null || _a === void 0 ? void 0 : _a.tableOrIndexName); }).forEach((cc) => {
                    var _a;
                    (_a = this.tokenBucket) === null || _a === void 0 ? void 0 : _a.take(cc.ReadCapacityUnits || cc.CapacityUnits || 0, true);
                });
            }
            else {
                (_a = this.tokenBucket) === null || _a === void 0 ? void 0 : _a.take(data.ConsumedCapacity.ReadCapacityUnits || data.ConsumedCapacity.CapacityUnits || 0, true);
            }
        }
    }
    setupFetchProcessor(promise) {
        this.activeRequests.push(promise);
        this.bufferSize += 1;
        return promise
            .then((data) => {
            this.removeCapacityFromTokenBucket(data);
            return data;
        })
            .then((data) => {
            this.activeRequests = this.activeRequests.filter((r) => r !== promise);
            this.processResult(data);
        })
            .catch((e) => {
            this.activeRequests = this.activeRequests.filter((r) => r !== promise);
            this.processError(e);
        });
    }
    // Entry point.
    async *execute() {
        let count = 0;
        do {
            if (this.errors) {
                return Promise.reject(this.errors);
            }
            // if there is no data to return, wait for data to exist
            if (!this.hasDataReady()) {
                await this.fetchNext();
            }
            // check for errors again after running another fetch
            if (this.errors) {
                return Promise.reject(this.errors);
            }
            const batch = this.getResultBatch(Math.min(this.batchSize, this.limit ? this.limit - count : 1000000000000));
            count += batch.length;
            if (!this.isDone() && (!this.limit || count < this.limit)) {
                // do not await here, background process the next set of data
                void this.fetchNext();
            }
            if (batch.length) {
                yield batch;
            }
            if (this.limit && count >= this.limit) {
                if (typeof this.nextToken === "object" && this.nextToken !== null) {
                    return { lastEvaluatedKey: this.nextToken };
                }
                return;
            }
        } while (!this.isDone());
    }
    getResultBatch(batchSize) {
        const items = (this.results.length && this.results.splice(0, batchSize)) || [];
        if (!items.length) {
            this.bufferSize = this.activeRequests.length;
        }
        else {
            this.bufferSize -= 1;
        }
        return items;
    }
    processError(e) {
        this.errors = e;
    }
    hasDataReady() {
        return this.results.length > 0;
    }
    isDone() {
        return !this.isActive() && this.nextToken === null && this.results.length === 0;
    }
    isActive() {
        return this.activeRequests.length > 0;
    }
}
exports.AbstractFetcher = AbstractFetcher;
