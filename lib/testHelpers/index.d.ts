/// <reference types="jest" />
import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Request } from "aws-sdk/lib/request";
declare type Spy<TInput, TOutput> = jest.MockContext<Request<TOutput, Error>, [TInput, any?]>;
declare type WrappedFn<TInput, TOutput> = (client: DocumentClient, spy: Spy<TInput, TOutput>) => Promise<void>;
declare type MockReturn<TOutput> = {
    err?: Error;
    data?: TOutput;
} | {
    err?: Error;
    data?: TOutput;
}[];
export declare function setMockOn(on: boolean): void;
interface MockSet<TOutput = {}> {
    name: "scan" | "query" | "delete" | "update" | "put" | "batchGet" | "batchWrite" | "transactGet";
    returns?: MockReturn<TOutput>;
    delay?: number;
}
export declare function multiMock(fn: (client: DocumentClient, spies: jest.MockContext<any, any[]>[]) => Promise<void>, mockSet: MockSet[]): () => Promise<void>;
export declare function mockScan(fn: WrappedFn<DocumentClient.ScanInput, DocumentClient.ScanOutput>, returns?: MockReturn<DocumentClient.ScanOutput>, delay?: number): () => Promise<void>;
export declare function alwaysMockScan(fn: WrappedFn<DocumentClient.ScanInput, DocumentClient.ScanOutput>, returns?: MockReturn<DocumentClient.ScanOutput>, delay?: number): () => Promise<void>;
export declare function mockQuery(fn: WrappedFn<DocumentClient.QueryInput, DocumentClient.QueryOutput>, returns?: MockReturn<DocumentClient.QueryOutput>, delay?: number): () => Promise<void>;
export declare function alwaysMockQuery(fn: WrappedFn<DocumentClient.QueryInput, DocumentClient.QueryOutput>, returns?: MockReturn<DocumentClient.QueryOutput>, delay?: number): () => Promise<void>;
export declare function alwaysMockBatchGet(fn: WrappedFn<DocumentClient.BatchGetItemInput, DocumentClient.BatchGetItemOutput>, returns?: MockReturn<DocumentClient.BatchGetItemOutput>, delay?: number): () => Promise<void>;
export declare function mockPut(fn: WrappedFn<DocumentClient.PutItemInput, DocumentClient.PutItemOutput>, returns?: MockReturn<DocumentClient.PutItemOutput>): () => Promise<void>;
export declare function mockUpdate(fn: WrappedFn<DocumentClient.UpdateItemInput, DocumentClient.UpdateItemOutput>, returns?: MockReturn<DocumentClient.UpdateItemOutput>): () => Promise<void>;
export declare function mockDelete(fn: WrappedFn<DocumentClient.DeleteItemInput, DocumentClient.DeleteItemOutput>, returns?: MockReturn<DocumentClient.DeleteItemOutput>): () => Promise<void>;
export declare function alwaysMockBatchWrite(fn: WrappedFn<DocumentClient.BatchWriteItemInput, DocumentClient.BatchWriteItemOutput>, returns?: MockReturn<DocumentClient.BatchWriteItemOutput>): () => Promise<void>;
export declare function mockBatchWrite(fn: WrappedFn<DocumentClient.BatchWriteItemInput, DocumentClient.BatchWriteItemOutput>, returns?: MockReturn<DocumentClient.BatchWriteItemOutput>, delay?: number): () => Promise<void>;
export declare function mockBatchGet(fn: WrappedFn<DocumentClient.BatchGetItemInput, DocumentClient.BatchGetItemOutput>, returns?: MockReturn<DocumentClient.BatchGetItemOutput>, delay?: number): () => Promise<void>;
export declare function mockTransactGet(fn: WrappedFn<DocumentClient.TransactGetItemsInput, DocumentClient.TransactGetItemsOutput>, returns?: MockReturn<DocumentClient.TransactGetItemsOutput>, delay?: number): () => Promise<void>;
export {};
