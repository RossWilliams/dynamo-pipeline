"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.mockTransactGet = exports.mockBatchGet = exports.mockBatchWrite = exports.alwaysMockBatchWrite = exports.mockDelete = exports.mockUpdate = exports.mockPut = exports.alwaysMockBatchGet = exports.alwaysMockQuery = exports.mockQuery = exports.alwaysMockScan = exports.mockScan = exports.multiMock = exports.setMockOn = void 0;
const lib_dynamodb_1 = require("@aws-sdk/lib-dynamodb");
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
let mockOn = true;
function setMockOn(on) {
    mockOn = on;
}
exports.setMockOn = setMockOn;
function multiMock(fn, mockSet) {
    return async () => {
        // eslint-disable-next-line
        const client = lib_dynamodb_1.DynamoDBDocumentClient.from(new client_dynamodb_1.DynamoDBClient({}));
        const spies = mockSet.map((ms) => setupMock(client, ms.name, ms.returns, true, ms.delay).mock);
        await fn(client, spies);
        teardownMock(client, true);
    };
}
exports.multiMock = multiMock;
function mockScan(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.ScanCommand, fn, returns, false, delay);
}
exports.mockScan = mockScan;
function alwaysMockScan(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.ScanCommand, fn, returns, true, delay);
}
exports.alwaysMockScan = alwaysMockScan;
function mockQuery(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.QueryCommand, fn, returns, false, delay);
}
exports.mockQuery = mockQuery;
function alwaysMockQuery(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.QueryCommand, fn, returns, true, delay);
}
exports.alwaysMockQuery = alwaysMockQuery;
function alwaysMockBatchGet(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.BatchGetCommand, fn, returns, true, delay);
}
exports.alwaysMockBatchGet = alwaysMockBatchGet;
function mockPut(fn, returns) {
    return mockCall(lib_dynamodb_1.PutCommand, fn, returns);
}
exports.mockPut = mockPut;
function mockUpdate(fn, returns) {
    return mockCall(lib_dynamodb_1.UpdateCommand, fn, returns);
}
exports.mockUpdate = mockUpdate;
function mockDelete(fn, returns) {
    return mockCall(lib_dynamodb_1.DeleteCommand, fn, returns);
}
exports.mockDelete = mockDelete;
function alwaysMockBatchWrite(fn, returns) {
    return mockCall(lib_dynamodb_1.BatchWriteCommand, fn, returns, true);
}
exports.alwaysMockBatchWrite = alwaysMockBatchWrite;
function mockBatchWrite(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.BatchWriteCommand, fn, returns, false, delay);
}
exports.mockBatchWrite = mockBatchWrite;
function mockBatchGet(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.BatchGetCommand, fn, returns, false, delay);
}
exports.mockBatchGet = mockBatchGet;
function mockTransactGet(fn, returns, delay) {
    return mockCall(lib_dynamodb_1.TransactGetCommand, fn, returns, false, delay);
}
exports.mockTransactGet = mockTransactGet;
function mockCall(name, fn, returns = {}, alwaysMock = false, delay) {
    return async () => {
        if (!mockOn && !alwaysMock) {
            const client = lib_dynamodb_1.DynamoDBDocumentClient.from(new client_dynamodb_1.DynamoDBClient({}));
            jest.spyOn(lib_dynamodb_1.DynamoDBDocumentClient, "from").mockImplementation(() => {
                return client;
            });
            await fn(client, jest.spyOn(client, "send").mock);
            teardownMock(client, alwaysMock);
        }
        else {
            const client = lib_dynamodb_1.DynamoDBDocumentClient.from(new client_dynamodb_1.DynamoDBClient({}));
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
        if (!jest.isMockFunction(lib_dynamodb_1.DynamoDBDocumentClient.from)) {
            // in addition mock the entire DynamoDBDocumentClient constructor, this allows
            // users who do not pass in a document client into the constructor to skill mock. See example-lambda.test.ts
            jest.spyOn(lib_dynamodb_1.DynamoDBDocumentClient, "from").mockImplementation(() => {
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
    lib_dynamodb_1.DynamoDBDocumentClient.from.mockRestore();
}
