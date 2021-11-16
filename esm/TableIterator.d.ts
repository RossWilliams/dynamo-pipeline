import { AttributeMap } from "./ScanQueryPipeline";
interface IteratorExecutor<T> {
    execute(): AsyncGenerator<T[], {
        lastEvaluatedKey: Record<string, unknown>;
    } | void, void>;
}
export declare class TableIterator<T = AttributeMap, P = undefined> {
    private lastEvaluatedKeyHandlers;
    config: {
        parent: P;
        fetcher: IteratorExecutor<T>;
    };
    constructor(fetcher: IteratorExecutor<T>, parent?: P);
    forEachStride(iterator: (items: T[], index: number, parent: P, cancel: () => void) => Promise<any> | void): Promise<P>;
    onLastEvaluatedKey(handler: (lastEvaluatedKey: Record<string, unknown>) => void): this;
    private iterate;
    private handleDone;
    forEach(iterator: (item: T, index: number, pipeline: P, cancel: () => void) => Promise<any> | void): Promise<P>;
    map<U>(iterator: (item: T, index: number) => U): Promise<U[]>;
    filterLazy(predicate: (item: T, index: number) => boolean): TableIterator<T, P>;
    mapLazy<U>(iterator: (item: T, index: number) => U): TableIterator<U, P>;
    all(): Promise<T[]>;
    iterator(): AsyncGenerator<T, void, void>;
    strideIterator(): AsyncGenerator<T[], Record<string, unknown> | void, void>;
}
export {};
