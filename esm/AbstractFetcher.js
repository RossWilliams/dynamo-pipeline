export class AbstractFetcher {
    constructor(client, options) {
        this.activeRequests = [];
        this.bufferSize = 0;
        this.bufferCapacity = 1;
        this.totalReturned = 0;
        this.results = [];
        this.errors = null;
        this.documentClient = client;
        this.bufferCapacity = options.bufferCapacity;
        this.batchSize = options.batchSize;
        this.limit = options.limit;
        this.nextToken = null;
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
