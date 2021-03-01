import DynamoDB from "aws-sdk/clients/dynamodb";
interface IteratorExecutor<T> {
    execute(): AsyncGenerator<T[], void, void>;
}
export declare class TableIterator<P, T = DynamoDB.AttributeMap> {
    config: {
        pipeline: P;
        fetcher: IteratorExecutor<T>;
    };
    constructor(pipeline: P, fetcher: IteratorExecutor<T>);
    forEachStride(iterator: (items: T[], index: number, pipeline: P, cancel: () => void) => Promise<any> | void): Promise<P>;
    forEach(iterator: (item: T, index: number, pipeline: P, cancel: () => void) => Promise<any> | void): Promise<P>;
    map<U>(iterator: (item: T, index: number) => U): Promise<U[]>;
    mapLazy<U>(iterator: (item: T, index: number) => U): TableIterator<P, U>;
    all(): Promise<T[]>;
    iterator(): AsyncGenerator<T, void, void>;
}
export {};
