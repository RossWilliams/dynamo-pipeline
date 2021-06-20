import { AbstractFetcher } from "./AbstractFetcher";
import { ScanInput, QueryInput, DocumentClient } from "aws-sdk/clients/dynamodb";
import { TokenBucket } from "./TokenBucket";
export declare class QueryFetcher<T> extends AbstractFetcher<T> {
    private request;
    private operation;
    constructor(request: ScanInput | QueryInput, client: DocumentClient, operation: "query" | "scan", options: {
        batchSize: number;
        bufferCapacity: number;
        limit?: number;
        nextToken?: DocumentClient.Key;
        tokenBucket?: TokenBucket;
    });
    fetchStrategy(): null | Promise<any>;
    processResult(data: DocumentClient.ScanOutput | DocumentClient.QueryOutput | void): void;
    getResultBatch(batchSize: number): T[];
}
