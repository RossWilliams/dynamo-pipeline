import { Pipeline } from "./pipeline";
import { BatchFetcher } from "./AbstractFetcher";
import DynamoDB from "aws-sdk/clients/dynamodb";
export declare class TableIterator<T = DynamoDB.AttributeMap> {
    config: {
        pipeline: Pipeline;
        fetcher: BatchFetcher<T>;
    };
    constructor(pipeline: Pipeline, fetcher: BatchFetcher<T>);
    forEach(iterator: (item: T, pipeline: Pipeline) => Promise<any> | false | void): Promise<Pipeline>;
    map<U>(iterator: (item: T, index: number) => U): Promise<U[]>;
    all(): Promise<T[]>;
}
