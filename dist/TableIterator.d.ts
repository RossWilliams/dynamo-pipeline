import { AbstractFetcher } from "./AbstractFetcher";
import DynamoDB from "aws-sdk/clients/dynamodb";
export declare class TableIterator<P, T = DynamoDB.AttributeMap> {
    config: {
        pipeline: P;
        fetcher: AbstractFetcher<T>;
    };
    constructor(pipeline: P, fetcher: AbstractFetcher<T>);
    forEach(iterator: (item: T, pipeline: P) => Promise<any> | false | void): Promise<P>;
    map<U>(iterator: (item: T, index: number) => U): Promise<U[]>;
    all(): Promise<T[]>;
}
