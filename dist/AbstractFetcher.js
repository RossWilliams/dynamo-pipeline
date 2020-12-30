"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AbstractFetcher = void 0;
class AbstractFetcher {
    constructor(client, options) {
        var _a, _b;
        this.activeRequests = [];
        this.bufferSize = 0;
        this.bufferCapacity = 1;
        this.totalReturned = 0;
        this.results = [];
        this.errors = null;
        this.documentClient = client;
        this.bufferCapacity = (_a = options.bufferCapacity) !== null && _a !== void 0 ? _a : 4;
        this.batchSize = (_b = options.batchSize) !== null && _b !== void 0 ? _b : 100;
        this.limit = options.limit;
    }
    // take in a promise to allow recursive calls,
    // batch fetcher can immediately create many requests
    fetchNext() {
        const fetchResponse = this.fetchStrategy();
        if (fetchResponse instanceof Promise && !this.activeRequests.includes(fetchResponse)) {
            return this.setupFetchProcessor(fetchResponse);
        }
        return fetchResponse;
    }
    setupFetchProcessor(promise) {
        this.activeRequests.push(promise);
        this.bufferSize += 1;
        return promise
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
            if (!this.hasDataReady()) {
                await this.fetchNext();
            }
            const batch = this.getResultBatch(Math.min(this.batchSize, this.limit ? this.limit - count : 100000));
            count += batch.length;
            if (!this.isDone() && (!this.limit || count < this.limit)) {
                // do not await here, background process the next set of data
                this.fetchNext();
            }
            yield batch;
            if (this.limit && count >= this.limit) {
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
