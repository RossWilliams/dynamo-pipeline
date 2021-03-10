import DynamoDB from "aws-sdk/clients/dynamodb";
interface IteratorExecutor<T> {
    execute(): AsyncGenerator<T[], void, void>;
}
export declare class TableIterator<T = DynamoDB.AttributeMap, P = undefined> {
    config: {
        parent: P;
        fetcher: IteratorExecutor<T>;
    };
    constructor(fetcher: IteratorExecutor<T>, parent?: P);
    forEachStride(iterator: (items: T[], index: number, parent: P, cancel: () => void) => Promise<any> | void): Promise<P>;
    forEach(iterator: (item: T, index: number, pipeline: P, cancel: () => void) => Promise<any> | void): Promise<P>;
    map<U>(iterator: (item: T, index: number) => U): Promise<U[]>;
    filterLazy(predicate: (item: T, index: number) => boolean): TableIterator<T, P>;
    mapLazy<U>(iterator: (item: T, index: number) => U): TableIterator<U, P>;
    all(): Promise<T[]>;
    iterator(): AsyncGenerator<T, void, void>;
    strideIterator(): AsyncGenerator<T[], void, void>;
}
export {};
