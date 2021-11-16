/// <reference types="jest" />
import { BatchGetCommand, BatchGetCommandOutput, BatchWriteCommand, BatchWriteCommandOutput, DeleteCommand, DeleteCommandOutput, DynamoDBDocumentClient, PutCommand, PutCommandOutput, QueryCommand, QueryCommandOutput, ScanCommand, ScanCommandOutput, TransactGetCommand, TransactGetCommandOutput, UpdateCommand, UpdateCommandOutput } from "@aws-sdk/lib-dynamodb";
declare type Spy<TInput, TOutput> = jest.MockContext<Promise<TOutput>, [TInput]>;
declare type WrappedFn<TInput, TOutput> = (client: DynamoDBDocumentClient, spy: Spy<TInput, TOutput>) => Promise<void>;
declare type MockReturn<TOutput> = {
    err?: Error;
    data?: Omit<TOutput, "$metadata">;
} | {
    err?: Error;
    data?: Omit<TOutput, "$metadata">;
}[];
declare type CommandTypes = typeof ScanCommand | typeof QueryCommand | typeof DeleteCommand | typeof UpdateCommand | typeof PutCommand | typeof BatchGetCommand | typeof BatchWriteCommand | typeof TransactGetCommand;
export declare function setMockOn(on: boolean): void;
interface MockSet<TOutput = Record<string, unknown>> {
    name: CommandTypes;
    returns?: MockReturn<TOutput>;
    delay?: number;
}
export declare function multiMock(fn: (client: DynamoDBDocumentClient, spies: jest.MockContext<any, any[]>[]) => Promise<void>, mockSet: MockSet<Record<string, unknown>>[]): () => Promise<void>;
export declare function mockScan(fn: WrappedFn<ScanCommand, ScanCommandOutput>, returns?: MockReturn<ScanCommandOutput>, delay?: number): () => Promise<void>;
export declare function alwaysMockScan(fn: WrappedFn<ScanCommand, ScanCommandOutput>, returns?: MockReturn<ScanCommandOutput>, delay?: number): () => Promise<void>;
export declare function mockQuery(fn: WrappedFn<QueryCommand, QueryCommandOutput>, returns?: MockReturn<QueryCommandOutput>, delay?: number): () => Promise<void>;
export declare function alwaysMockQuery(fn: WrappedFn<QueryCommand, QueryCommandOutput>, returns?: MockReturn<QueryCommandOutput>, delay?: number): () => Promise<void>;
export declare function alwaysMockBatchGet(fn: WrappedFn<BatchGetCommand, BatchGetCommandOutput>, returns?: MockReturn<BatchGetCommandOutput>, delay?: number): () => Promise<void>;
export declare function mockPut(fn: WrappedFn<PutCommand, PutCommandOutput>, returns?: MockReturn<PutCommandOutput>): () => Promise<void>;
export declare function mockUpdate(fn: WrappedFn<UpdateCommand, UpdateCommandOutput>, returns?: MockReturn<UpdateCommandOutput>): () => Promise<void>;
export declare function mockDelete(fn: WrappedFn<DeleteCommand, DeleteCommandOutput>, returns?: MockReturn<DeleteCommandOutput>): () => Promise<void>;
export declare function alwaysMockBatchWrite(fn: WrappedFn<BatchWriteCommand, BatchWriteCommandOutput>, returns?: MockReturn<BatchWriteCommandOutput>): () => Promise<void>;
export declare function mockBatchWrite(fn: WrappedFn<BatchWriteCommand, BatchWriteCommandOutput>, returns?: MockReturn<BatchWriteCommandOutput>, delay?: number): () => Promise<void>;
export declare function mockBatchGet(fn: WrappedFn<BatchGetCommand, BatchGetCommandOutput>, returns?: MockReturn<BatchGetCommandOutput>, delay?: number): () => Promise<void>;
export declare function mockTransactGet(fn: WrappedFn<TransactGetCommand, TransactGetCommandOutput>, returns?: MockReturn<TransactGetCommandOutput>, delay?: number): () => Promise<void>;
export {};
