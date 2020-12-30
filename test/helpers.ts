import { DocumentClient } from "aws-sdk/clients/dynamodb";
import AWS from "aws-sdk";
import { Request } from "aws-sdk/lib/request";
import AWSMock from "aws-sdk-mock";

let mockOn = true;

type Spy<TInput, TOutput> = jest.MockContext<Request<TOutput, Error>, [TInput, any?]>;
type WrappedFn<TInput, TOutput> = (client: DocumentClient, spy: Spy<TInput, TOutput>) => Promise<void>;
type MockReturn<TOutput> = { err?: Error; data?: TOutput } | { err?: Error; data?: TOutput }[];

export function setMockOn(on: boolean): void {
  mockOn = on;
}

export function mockScan(
  fn: WrappedFn<DocumentClient.ScanInput, DocumentClient.ScanOutput>,
  returns?: MockReturn<DocumentClient.ScanOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("scan", fn, returns, false, delay);
}

export function alwaysMockScan(
  fn: WrappedFn<DocumentClient.ScanInput, DocumentClient.ScanOutput>,
  returns?: MockReturn<DocumentClient.ScanOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("scan", fn, returns, true, delay);
}

export function mockQuery(
  fn: WrappedFn<DocumentClient.QueryInput, DocumentClient.QueryOutput>,
  returns?: MockReturn<DocumentClient.QueryOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("query", fn, returns, false, delay);
}

export function alwaysMockQuery(
  fn: WrappedFn<DocumentClient.QueryInput, DocumentClient.QueryOutput>,
  returns?: MockReturn<DocumentClient.QueryOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("query", fn, returns, true, delay);
}

export function alwaysMockBatchGet(
  fn: WrappedFn<DocumentClient.BatchGetItemInput, DocumentClient.BatchGetItemOutput>,
  returns?: MockReturn<DocumentClient.BatchGetItemOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("batchGet", fn, returns, true, delay);
}

export function mockPut(
  fn: WrappedFn<DocumentClient.PutItemInput, DocumentClient.PutItemOutput>,
  returns?: MockReturn<DocumentClient.PutItemOutput>
): () => Promise<void> {
  return mockCall("put", fn, returns);
}

export function mockUpdate(
  fn: WrappedFn<DocumentClient.UpdateItemInput, DocumentClient.UpdateItemOutput>,
  returns?: MockReturn<DocumentClient.UpdateItemOutput>
): () => Promise<void> {
  return mockCall("update", fn, returns);
}

export function mockDelete(
  fn: WrappedFn<DocumentClient.DeleteItemInput, DocumentClient.DeleteItemOutput>,
  returns?: MockReturn<DocumentClient.DeleteItemOutput>
): () => Promise<void> {
  return mockCall("delete", fn, returns);
}

export function alwaysMockBatchWrite(
  fn: WrappedFn<DocumentClient.BatchWriteItemInput, DocumentClient.BatchWriteItemOutput>,
  returns?: MockReturn<DocumentClient.BatchWriteItemOutput>
): () => Promise<void> {
  return mockCall("batchWrite", fn, returns, true);
}

export function mockBatchWrite(
  fn: WrappedFn<DocumentClient.BatchWriteItemInput, DocumentClient.BatchWriteItemOutput>,
  returns?: MockReturn<DocumentClient.BatchWriteItemOutput>
): () => Promise<void> {
  return mockCall("batchWrite", fn, returns);
}

export function mockBatchGet(
  fn: WrappedFn<DocumentClient.BatchGetItemInput, DocumentClient.BatchGetItemOutput>,
  returns?: MockReturn<DocumentClient.BatchGetItemOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("batchGet", fn, returns, false, delay);
}

export function mockTransactGet(
  fn: WrappedFn<DocumentClient.TransactGetItemsInput, DocumentClient.TransactGetItemsOutput>,
  returns?: MockReturn<DocumentClient.TransactGetItemsOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall("transactGet", fn, returns, false, delay);
}

function mockCall<TInput, TOutput>(
  name: string,
  fn: WrappedFn<TInput, TOutput>,
  returns: MockReturn<TOutput> = {},
  alwaysMock = false,
  delay?: number
) {
  return async () => {
    const spy = jest.fn<Request<TOutput, Error>, [TInput, any?]>();
    let callCount = 0;
    if (mockOn || alwaysMock) {
      AWSMock.setSDKInstance(AWS);
      AWSMock.mock("DynamoDB.DocumentClient", name, function (input: TInput, callback: (err: any, args: any) => void) {
        spy(input);
        if (Array.isArray(returns)) {
          if (typeof delay === "number") {
            setTimeout(() => {
              callback(returns[callCount]?.err, returns[callCount]?.data);
              callCount += 1;
            }, delay);
          } else {
            callback(returns[callCount]?.err, returns[callCount]?.data);
            callCount += 1;
          }
        } else if (typeof delay === "number") {
          setTimeout(() => callback(returns?.err, returns?.data), delay);
        } else {
          callback(returns?.err, returns?.data);
        }
      });
    }

    // TODO: Type cleanup
    const client = new (AWS as any).DynamoDB.DocumentClient();

    if (!mockOn && !alwaysMock) {
      // TODO: Type cleanup
      await fn(
        client,
        jest.spyOn<{ method: (arg0: TInput) => Request<TOutput, Error> }, "method">(client, name as "method").mock
      );
    } else {
      await fn(client, spy.mock);
    }

    if (mockOn || alwaysMock) {
      AWSMock.restore("DynamoDB.DocumentClient");
    }
  };
}
