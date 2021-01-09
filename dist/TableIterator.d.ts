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
    forEach(iterator: (item: T, index: number, pipeline: P) => Promise<any> | false | void): Promise<P>;
    map<U>(iterator: (item: T, index: number) => U): Promise<U[]>;
    all(): Promise<T[]>;
}
export {};
