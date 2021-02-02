"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueryFetcher = void 0;
const AbstractFetcher_1 = require("./AbstractFetcher");
class QueryFetcher extends AbstractFetcher_1.AbstractFetcher {
    constructor(request, client, operation, options) {
        super(client, options);
        this.request = request;
        this.operation = operation;
        this.nextToken = true;
    }
    // TODO: remove null response type
    fetchStrategy() {
        // no support for parallel query
        // 1. 1 active request allowed at a time
        // 2. Do not create a new request when the buffer is full
        // 3. If there are no more items to fetch, exit
        if (this.activeRequests.length > 0 || this.bufferSize > this.bufferCapacity || !this.nextToken) {
            return this.activeRequests[0] || null;
        }
        const request = {
            ...(this.request.Limit && { Limit: this.request.Limit - this.totalReturned }),
            ...this.request,
            ...(this.nextToken && this.nextToken !== true && { ExclusiveStartKey: this.nextToken }),
        };
        const promise = this.documentClient[this.operation](request).promise();
        return promise;
    }
    processResult(data) {
        this.nextToken = (data && data.LastEvaluatedKey) || null;
        if (data && data.Items) {
            this.totalReturned += data.Items.length;
            this.results.push(...data.Items);
        }
    }
    // override since filtering results in inconsistent result set size, base buffer on the items returned last
    // this may give surprising results if the returned list varies considerably, but errs on the side of caution.
    getResultBatch(batchSize) {
        const items = super.getResultBatch(batchSize);
        if (items.length) {
            this.bufferSize = (this.results || []).length / (items.length || 1);
        }
        else if (!this.activeRequests.length) {
            // if we don't have any items to process, and no active requests, buffer size should be zero.
            this.bufferSize = 0;
        }
        return items;
    }
}
exports.QueryFetcher = QueryFetcher;
