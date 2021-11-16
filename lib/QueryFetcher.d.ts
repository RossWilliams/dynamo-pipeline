import { AbstractFetcher } from "./AbstractFetcher";
import { DynamoDBDocumentClient as DocumentClient, ScanCommandOutput, QueryCommandOutput, ScanCommandInput, QueryCommandInput } from "@aws-sdk/lib-dynamodb";
export declare class QueryFetcher<T> extends AbstractFetcher<T> {
    private request;
    private operation;
    constructor(request: ScanCommandInput | QueryCommandInput, client: DocumentClient, operation: "query" | "scan", options: {
        batchSize: number;
        bufferCapacity: number;
        limit?: number;
        nextToken?: Record<string, unknown>;
    });
    fetchStrategy(): null | Promise<any>;
    processResult(data: ScanCommandOutput | QueryCommandOutput | void): void;
    getResultBatch(batchSize: number): T[];
}
