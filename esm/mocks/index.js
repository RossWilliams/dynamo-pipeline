import { BatchGetCommand, BatchWriteCommand, DeleteCommand, DynamoDBDocumentClient, PutCommand, QueryCommand, ScanCommand, TransactGetCommand, UpdateCommand, } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
let mockOn = true;
export function setMockOn(on) {
    mockOn = on;
}
export function multiMock(fn, mockSet) {
    return async () => {
        // eslint-disable-next-line
        const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
        const spies = mockSet.map((ms) => setupMock(client, ms.name, ms.returns, true, ms.delay).mock);
        await fn(client, spies);
        teardownMock(client, true);
    };
}
export function mockScan(fn, returns, delay) {
    return mockCall(ScanCommand, fn, returns, false, delay);
}
export function alwaysMockScan(fn, returns, delay) {
    return mockCall(ScanCommand, fn, returns, true, delay);
}
export function mockQuery(fn, returns, delay) {
    return mockCall(QueryCommand, fn, returns, false, delay);
}
export function alwaysMockQuery(fn, returns, delay) {
    return mockCall(QueryCommand, fn, returns, true, delay);
}
export function alwaysMockBatchGet(fn, returns, delay) {
    return mockCall(BatchGetCommand, fn, returns, true, delay);
}
export function mockPut(fn, returns) {
    return mockCall(PutCommand, fn, returns);
}
export function mockUpdate(fn, returns) {
    return mockCall(UpdateCommand, fn, returns);
}
export function mockDelete(fn, returns) {
    return mockCall(DeleteCommand, fn, returns);
}
export function alwaysMockBatchWrite(fn, returns) {
    return mockCall(BatchWriteCommand, fn, returns, true);
}
export function mockBatchWrite(fn, returns, delay) {
    return mockCall(BatchWriteCommand, fn, returns, false, delay);
}
export function mockBatchGet(fn, returns, delay) {
    return mockCall(BatchGetCommand, fn, returns, false, delay);
}
export function mockTransactGet(fn, returns, delay) {
    return mockCall(TransactGetCommand, fn, returns, false, delay);
}
function mockCall(name, fn, returns = {}, alwaysMock = false, delay) {
    return async () => {
        if (!mockOn && !alwaysMock) {
            const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
            jest.spyOn(DynamoDBDocumentClient, "from").mockImplementation(() => {
                return client;
            });
            await fn(client, jest.spyOn(client, "send").mock);
            teardownMock(client, alwaysMock);
        }
        else {
            const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
            const spy = setupMock(client, name, returns, alwaysMock, delay);
            await fn(client, spy.mock);
            teardownMock(client, alwaysMock);
        }
    };
}
function setupMock(client, name, returns = {}, alwaysMock, delay) {
    // setup a spy for both mocked and non-mocked scenarios
    const spy = jest.fn();
    // array iterator when 'returns' includes an array
    let callCount = 0;
    const callback = (...args) => {
        return new Promise((resolve, reject) => {
            var _a, _b, _c;
            // only process return values with the command name matches with the command being sent
            if (args[0] instanceof name) {
                if (Array.isArray(returns)) {
                    if (typeof delay === "number") {
                        setTimeout(() => {
                            var _a, _b, _c;
                            ((_a = returns[callCount]) === null || _a === void 0 ? void 0 : _a.err) ? reject((_b = returns[callCount]) === null || _b === void 0 ? void 0 : _b.err) : resolve((_c = returns[callCount]) === null || _c === void 0 ? void 0 : _c.data);
                            callCount += 1;
                        }, delay);
                    }
                    else {
                        ((_a = returns[callCount]) === null || _a === void 0 ? void 0 : _a.err) ? reject((_b = returns[callCount]) === null || _b === void 0 ? void 0 : _b.err) : resolve((_c = returns[callCount]) === null || _c === void 0 ? void 0 : _c.data);
                        callCount += 1;
                    }
                }
                else if (typeof delay === "number") {
                    setTimeout(() => {
                        (returns === null || returns === void 0 ? void 0 : returns.err) ? reject(returns === null || returns === void 0 ? void 0 : returns.err) : resolve(returns === null || returns === void 0 ? void 0 : returns.data);
                    }, delay);
                }
                else {
                    (returns === null || returns === void 0 ? void 0 : returns.err) ? reject(returns === null || returns === void 0 ? void 0 : returns.err) : resolve(returns === null || returns === void 0 ? void 0 : returns.data);
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
        }
        else {
            const fn = client.send.getMockImplementation();
            client.send.mockImplementation((...args) => {
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
function teardownMock(client, alwaysMock) {
    if (jest.isMockFunction(client.send)) {
        client.send.mockRestore();
    }
    DynamoDBDocumentClient.from.mockRestore();
}
