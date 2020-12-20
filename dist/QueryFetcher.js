"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueryFetcher = void 0;
const AbstractFetcher_1 = require("./AbstractFetcher");
class QueryFetcher extends AbstractFetcher_1.BatchFetcher {
    constructor(request, client, operation, batchSize = 100, bufferCapacity = 4, limit) {
        super(client, bufferCapacity, batchSize, limit);
        this.request = request;
        this.operation = operation;
        this.nextToken = true;
    }
    fetchStrategy() {
        if (this.activeRequests.length > 0 || this.bufferSize > this.bufferCapacity || !this.nextToken) {
            return this.activeRequests[0] || null;
        }
        const promise = this.documentClient[this.operation]({
            ...(this.request.Limit && { Limit: this.request.Limit - this.totalReturned }),
            ...this.request,
            ...(this.nextToken && this.nextToken !== true && { ExclusiveStartKey: this.nextToken }),
        }).promise();
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
    // this may give surprising results if the returned list varies considerable, but errs on the side of caution.
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
