import { Command } from "@aws-sdk/types";
import {
  BatchGetCommand,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandOutput,
  DeleteCommand,
  DeleteCommandOutput,
  DynamoDBDocumentClient,
  PutCommand,
  PutCommandOutput,
  QueryCommand,
  QueryCommandOutput,
  ScanCommand,
  ScanCommandOutput,
  ServiceInputTypes,
  ServiceOutputTypes,
  TransactGetCommand,
  TransactGetCommandOutput,
  UpdateCommand,
  UpdateCommandOutput,
} from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

let mockOn = true;

type Spy<TInput, TOutput> = jest.MockContext<Promise<TOutput>, [TInput]>;
type WrappedFn<TInput, TOutput> = (client: DynamoDBDocumentClient, spy: Spy<TInput, TOutput>) => Promise<void>;
type MockReturn<TOutput> =
  | { err?: Error; data?: Omit<TOutput, "$metadata"> }
  | { err?: Error; data?: Omit<TOutput, "$metadata"> }[];

type CommandTypes =
  | typeof ScanCommand
  | typeof QueryCommand
  | typeof DeleteCommand
  | typeof UpdateCommand
  | typeof PutCommand
  | typeof BatchGetCommand
  | typeof BatchWriteCommand
  | typeof TransactGetCommand;

export function setMockOn(on: boolean): void {
  mockOn = on;
}

interface MockSet<TOutput = Record<string, unknown>> {
  name: CommandTypes;
  returns?: MockReturn<TOutput>;
  delay?: number;
}

export function multiMock(
  fn: (client: DynamoDBDocumentClient, spies: jest.MockContext<any, any[]>[]) => Promise<void>,
  mockSet: MockSet<Record<string, unknown>>[]
): () => Promise<void> {
  return async () => {
    // eslint-disable-next-line
    const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
    const spies = mockSet.map((ms) => setupMock(client, ms.name, ms.returns, true, ms.delay).mock);

    await fn(client, spies);
    teardownMock(client, true);
  };
}

export function mockScan(
  fn: WrappedFn<ScanCommand, ScanCommandOutput>,
  returns?: MockReturn<ScanCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(ScanCommand, fn, returns, false, delay);
}

export function alwaysMockScan(
  fn: WrappedFn<ScanCommand, ScanCommandOutput>,
  returns?: MockReturn<ScanCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(ScanCommand, fn, returns, true, delay);
}

export function mockQuery(
  fn: WrappedFn<QueryCommand, QueryCommandOutput>,
  returns?: MockReturn<QueryCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(QueryCommand, fn, returns, false, delay);
}

export function alwaysMockQuery(
  fn: WrappedFn<QueryCommand, QueryCommandOutput>,
  returns?: MockReturn<QueryCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(QueryCommand, fn, returns, true, delay);
}

export function alwaysMockBatchGet(
  fn: WrappedFn<BatchGetCommand, BatchGetCommandOutput>,
  returns?: MockReturn<BatchGetCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(BatchGetCommand, fn, returns, true, delay);
}

export function mockPut(
  fn: WrappedFn<PutCommand, PutCommandOutput>,
  returns?: MockReturn<PutCommandOutput>
): () => Promise<void> {
  return mockCall(PutCommand, fn, returns);
}

export function mockUpdate(
  fn: WrappedFn<UpdateCommand, UpdateCommandOutput>,
  returns?: MockReturn<UpdateCommandOutput>
): () => Promise<void> {
  return mockCall(UpdateCommand, fn, returns);
}

export function mockDelete(
  fn: WrappedFn<DeleteCommand, DeleteCommandOutput>,
  returns?: MockReturn<DeleteCommandOutput>
): () => Promise<void> {
  return mockCall(DeleteCommand, fn, returns);
}

export function alwaysMockBatchWrite(
  fn: WrappedFn<BatchWriteCommand, BatchWriteCommandOutput>,
  returns?: MockReturn<BatchWriteCommandOutput>
): () => Promise<void> {
  return mockCall(BatchWriteCommand, fn, returns, true);
}

export function mockBatchWrite(
  fn: WrappedFn<BatchWriteCommand, BatchWriteCommandOutput>,
  returns?: MockReturn<BatchWriteCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(BatchWriteCommand, fn, returns, false, delay);
}

export function mockBatchGet(
  fn: WrappedFn<BatchGetCommand, BatchGetCommandOutput>,
  returns?: MockReturn<BatchGetCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(BatchGetCommand, fn, returns, false, delay);
}

export function mockTransactGet(
  fn: WrappedFn<TransactGetCommand, TransactGetCommandOutput>,
  returns?: MockReturn<TransactGetCommandOutput>,
  delay?: number
): () => Promise<void> {
  return mockCall(TransactGetCommand, fn, returns, false, delay);
}

function mockCall<TInput extends Command<ServiceInputTypes, any, ServiceOutputTypes, any, any>, TOutput>(
  name: CommandTypes,
  fn: WrappedFn<TInput, TOutput>,
  returns: MockReturn<TOutput> = {},
  alwaysMock = false,
  delay?: number
) {
  return async () => {
    if (!mockOn && !alwaysMock) {
      const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
      jest.spyOn(DynamoDBDocumentClient, "from").mockImplementation(() => {
        return client;
      });
      await fn(client, jest.spyOn<{ send: (arg0: TInput) => Promise<TOutput> }, "send">(client, "send" as const).mock);
      teardownMock(client, alwaysMock);
    } else {
      const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
      const spy = setupMock(client, name, returns, alwaysMock, delay);
      await fn(client, spy.mock);
      teardownMock(client, alwaysMock);
    }
  };
}

function setupMock<TOutput>(
  client: DynamoDBDocumentClient,
  name: CommandTypes,
  returns: MockReturn<TOutput> = {},
  alwaysMock: boolean,
  delay?: number
) {
  // setup a spy for both mocked and non-mocked scenarios
  const spy = jest.fn();

  // array iterator when 'returns' includes an array
  let callCount = 0;

  const callback = (...args: Parameters<typeof client.send>): Promise<any> => {
    return new Promise((resolve, reject) => {
      // only process return values with the command name matches with the command being sent
      if (args[0] instanceof name) {
        if (Array.isArray(returns)) {
          if (typeof delay === "number") {
            setTimeout(() => {
              returns[callCount]?.err ? reject(returns[callCount]?.err) : resolve(returns[callCount]?.data);
              callCount += 1;
            }, delay);
          } else {
            returns[callCount]?.err ? reject(returns[callCount]?.err) : resolve(returns[callCount]?.data);
            callCount += 1;
          }
        } else if (typeof delay === "number") {
          setTimeout(() => {
            returns?.err ? reject(returns?.err) : resolve(returns?.data);
          }, delay);
        } else {
          returns?.err ? reject(returns?.err) : resolve(returns?.data);
        }
      }
    });
  };

  if (mockOn || alwaysMock) {
    // mock the send call, but only once
    if (!jest.isMockFunction(client.send)) {
      jest.spyOn(client, "send").mockImplementation((...args) => {
        // trigger the spy to record inputs to the request, but only when the command matches the passed in name
        if (args[0] instanceof name) {
          spy(...args);
        }
        return callback(...args);
      });
    } else {
      const fn = (client.send as jest.Mock).getMockImplementation();
      (client.send as jest.Mock).mockImplementation((...args: Parameters<typeof client.send>) => {
        if (args[0] instanceof name) {
          spy(...args);
        }
        const exstingPromise = fn(...args);
        // only a promise with a command that matches the command name will resolve, but
        // we run all promises. Race gives us the value of the only command that will return
        return Promise.race([callback(...args), exstingPromise]);
      });
    }

    if (!jest.isMockFunction(DynamoDBDocumentClient.from)) {
      // in addition mock the entire DynamoDBDocumentClient constructor, this allows
      // users who do not pass in a document client into the constructor to skill mock. See example-lambda.test.ts
      jest.spyOn(DynamoDBDocumentClient, "from").mockImplementation(() => {
        return client;
      });
    }
  }

  return spy;
}

function teardownMock(client: DynamoDBDocumentClient, alwaysMock?: boolean) {
  if (jest.isMockFunction(client.send)) {
    (client.send as jest.Mock).mockRestore();
  }

  (DynamoDBDocumentClient.from as jest.Mock).mockRestore();
}
