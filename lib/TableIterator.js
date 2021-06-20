"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TableIterator = void 0;
class TableIterator {
    constructor(fetcher, parent, tokenBucket) {
        this.lastEvaluatedKeyHandlers = [];
        this.config = { parent: parent, fetcher };
        this.tokenBucket = tokenBucket;
    }
    async forEachStride(iterator) {
        let index = 0;
        await this.iterate(this.config.fetcher, async (stride, cancel) => {
            await iterator(stride, index, this.config.parent, cancel);
            index += 1;
        });
        return this.config.parent;
    }
    onLastEvaluatedKey(handler) {
        this.lastEvaluatedKeyHandlers.push(handler);
        return this;
    }
    async iterate(fetcher, iterator) {
        let cancelled = false;
        const cancel = () => {
            cancelled = true;
        };
        const executor = fetcher.execute();
        while (true) {
            if (cancelled) {
                break;
            }
            const stride = await executor.next();
            const { value } = stride;
            if (stride.done) {
                this.handleDone(stride);
                break;
            }
            await iterator(value, cancel);
        }
    }
    handleDone(iteratorResponse) {
        const { value } = iteratorResponse;
        if (value && "lastEvaluatedKey" in value) {
            this.lastEvaluatedKeyHandlers.forEach((h) => h(value.lastEvaluatedKey));
            this.lastEvaluatedKeyHandlers = [];
        }
    }
    // when a promise is returned, all promises are resolved in the batch before processing the next batch
    async forEach(iterator) {
        let index = 0;
        let iteratorPromises = [];
        let cancelled = false;
        const cancelForEach = () => {
            cancelled = true;
        };
        await this.iterate(this.config.fetcher, async (stride, cancel) => {
            iteratorPromises = [];
            for (const item of stride) {
                const iteratorResponse = iterator(item, index, this.config.parent, cancelForEach);
                index += 1;
                if (cancelled) {
                    await Promise.all(iteratorPromises);
                    cancel();
                    break;
                }
                else if (typeof iteratorResponse === "object" && iteratorResponse instanceof Promise) {
                    iteratorPromises.push(iteratorResponse);
                }
            }
            await Promise.all(iteratorPromises);
        });
        await Promise.all(iteratorPromises);
        return this.config.parent;
    }
    async map(iterator) {
        const results = [];
        let index = 0;
        await this.iterate(this.config.fetcher, (stride, _cancel) => {
            for (const item of stride) {
                results.push(iterator(item, index));
                index += 1;
            }
            return Promise.resolve();
        });
        return results;
    }
    filterLazy(predicate) {
        const existingFetcher = this.config.fetcher;
        let index = 0;
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const that = this;
        const fetcher = async function* () {
            const executor = existingFetcher.execute();
            while (true) {
                const stride = await executor.next();
                if (stride.done) {
                    that.handleDone(stride);
                    break;
                }
                yield stride.value.filter((val, i) => {
                    const filtered = predicate(val, index);
                    index += 1;
                    return filtered;
                });
            }
        };
        return new TableIterator({ execute: fetcher }, this.config.parent);
    }
    mapLazy(iterator) {
        const existingFetcher = this.config.fetcher;
        let results = [];
        let index = 0;
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const that = this;
        const fetcher = async function* () {
            const executor = existingFetcher.execute();
            while (true) {
                const stride = await executor.next();
                if (stride.done) {
                    that.handleDone(stride);
                    break;
                }
                results = stride.value.map((item) => {
                    const result = iterator(item, index);
                    index += 1;
                    return result;
                });
                yield results;
            }
        };
        return new TableIterator({ execute: fetcher }, this.config.parent);
    }
    all() {
        const result = this.map((i) => i);
        return result;
    }
    async *iterator() {
        const executor = this.config.fetcher.execute();
        while (true) {
            const stride = await executor.next();
            if (stride.done) {
                this.handleDone(stride);
                return;
            }
            for (const item of stride.value) {
                yield item;
            }
        }
    }
    strideIterator() {
        return this.config.fetcher.execute();
    }
}
exports.TableIterator = TableIterator;
