"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BatchFetcher = void 0;
class BatchFetcher {
    constructor(client, bufferCapacity, batchSize, limit) {
        this.activeRequests = [];
        this.bufferSize = 0;
        this.bufferCapacity = 1;
        this.totalReturned = 0;
        this.results = [];
        this.documentClient = client;
        this.bufferCapacity = bufferCapacity;
        this.batchSize = batchSize;
        this.limit = limit;
    }
    // take in a promise to allow recursive calls
    fetchNext(promise) {
        if (promise) {
            this.setupFetchProcessor(promise);
        }
        const fetchResponse = this.fetchStrategy();
        if (fetchResponse instanceof Promise && !this.activeRequests.includes(fetchResponse)) {
            this.setupFetchProcessor(fetchResponse);
        }
        return fetchResponse;
    }
    // Entry point.
    async *execute() {
        let count = 0;
        do {
            if (!this.hasDataReady()) {
                await this.fetchNext();
            }
            const batch = this.getResultBatch(Math.min(this.batchSize, this.limit ? this.limit - count : 100000));
            count += batch.length;
            if (!this.isDone() && (!this.limit || count < this.limit)) {
                // do not await here.
                this.fetchNext();
            }
            yield batch;
            if (this.limit && count >= this.limit) {
                return;
            }
        } while (!this.isDone());
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
            console.error("Error: AWS Error,", e);
        });
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
exports.BatchFetcher = BatchFetcher;
