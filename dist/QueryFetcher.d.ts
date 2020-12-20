import { BatchFetcher } from "./AbstractFetcher";
import { ScanInput, QueryInput, DocumentClient } from "aws-sdk/clients/dynamodb";
export declare class QueryFetcher<T> extends BatchFetcher<T> {
    private request;
    private operation;
    constructor(request: ScanInput | QueryInput, client: DocumentClient, operation: "query" | "scan", batchSize?: number, bufferCapacity?: number, limit?: number);
    fetchStrategy(): null | Promise<any>;
    processResult(data: DocumentClient.ScanOutput | DocumentClient.QueryOutput | void): void;
    getResultBatch(batchSize: number): T[];
}
