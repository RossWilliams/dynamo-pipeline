"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TableIterator = void 0;
class TableIterator {
    constructor(pipeline, fetcher) {
        this.config = { pipeline, fetcher };
    }
    async forEachStride(iterator) {
        let index = 0;
        const executor = this.config.fetcher.execute();
        let cancelled = false;
        const cancel = () => {
            cancelled = true;
        };
        for await (const stride of executor) {
            await iterator(stride, index, this.config.pipeline, cancel);
            index += 1;
            if (cancelled) {
                break;
            }
        }
        return this.config.pipeline;
    }
    // when a promise is returned, all promises are resolved in the batch before processing the next batch
    async forEach(iterator) {
        let index = 0;
        let iteratorPromises = [];
        const executor = this.config.fetcher.execute();
        let cancelled = false;
        const cancel = () => {
            cancelled = true;
        };
        // eslint-disable-next-line no-labels
        strides: for await (const stride of executor) {
            iteratorPromises = [];
            for (const item of stride) {
                const iteratorResponse = iterator(item, index, this.config.pipeline, cancel);
                index += 1;
                if (cancelled) {
                    await Promise.all(iteratorPromises);
                    // eslint-disable-next-line no-labels
                    break strides;
                }
                else if (typeof iteratorResponse === "object" && iteratorResponse instanceof Promise) {
                    iteratorPromises.push(iteratorResponse);
                }
            }
            await Promise.all(iteratorPromises);
        }
        await Promise.all(iteratorPromises);
        return this.config.pipeline;
    }
    async map(iterator) {
        const results = [];
        const executor = this.config.fetcher.execute();
        let index = 0;
        for await (const stride of executor) {
            for (const item of stride) {
                results.push(iterator(item, index));
                index += 1;
            }
        }
        return results;
    }
    mapLazy(iterator) {
        const existingFetcher = this.config.fetcher;
        let results = [];
        let index = 0;
        const fetcher = async function* () {
            const executor = existingFetcher.execute();
            for await (const stride of executor) {
                results = stride.map((item) => {
                    const result = iterator(item, index);
                    index += 1;
                    return result;
                });
                yield results;
            }
        };
        return new TableIterator(this.config.pipeline, { execute: fetcher });
    }
    all() {
        const result = this.map((i) => i);
        return result;
    }
    async *iterator() {
        const executor = this.config.fetcher.execute();
        for await (const stride of executor) {
            for (const item of stride) {
                yield item;
            }
        }
    }
}
exports.TableIterator = TableIterator;
