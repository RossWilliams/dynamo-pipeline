"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.mockTransactGet = exports.mockBatchGet = exports.mockBatchWrite = exports.alwaysMockBatchWrite = exports.mockDelete = exports.mockUpdate = exports.mockPut = exports.alwaysMockBatchGet = exports.alwaysMockQuery = exports.mockQuery = exports.alwaysMockScan = exports.mockScan = exports.multiMock = exports.setMockOn = void 0;
const aws_sdk_1 = __importDefault(require("aws-sdk"));
const aws_sdk_mock_1 = __importDefault(require("aws-sdk-mock"));
let mockOn = true;
function setMockOn(on) {
    mockOn = on;
}
exports.setMockOn = setMockOn;
function multiMock(fn, mockSet) {
    return async () => {
        const spies = mockSet.map((ms) => setupMock(ms.name, ms.returns, true, ms.delay).mock);
        const client = new aws_sdk_1.default.DynamoDB.DocumentClient();
        await fn(client, spies);
        mockSet.forEach((ms) => teardownMock(ms.name, true));
    };
}
exports.multiMock = multiMock;
function mockScan(fn, returns, delay) {
    return mockCall("scan", fn, returns, false, delay);
}
exports.mockScan = mockScan;
function alwaysMockScan(fn, returns, delay) {
    return mockCall("scan", fn, returns, true, delay);
}
exports.alwaysMockScan = alwaysMockScan;
function mockQuery(fn, returns, delay) {
    return mockCall("query", fn, returns, false, delay);
}
exports.mockQuery = mockQuery;
function alwaysMockQuery(fn, returns, delay) {
    return mockCall("query", fn, returns, true, delay);
}
exports.alwaysMockQuery = alwaysMockQuery;
function alwaysMockBatchGet(fn, returns, delay) {
    return mockCall("batchGet", fn, returns, true, delay);
}
exports.alwaysMockBatchGet = alwaysMockBatchGet;
function mockPut(fn, returns) {
    return mockCall("put", fn, returns);
}
exports.mockPut = mockPut;
function mockUpdate(fn, returns) {
    return mockCall("update", fn, returns);
}
exports.mockUpdate = mockUpdate;
function mockDelete(fn, returns) {
    return mockCall("delete", fn, returns);
}
exports.mockDelete = mockDelete;
function alwaysMockBatchWrite(fn, returns) {
    return mockCall("batchWrite", fn, returns, true);
}
exports.alwaysMockBatchWrite = alwaysMockBatchWrite;
function mockBatchWrite(fn, returns, delay) {
    return mockCall("batchWrite", fn, returns, false, delay);
}
exports.mockBatchWrite = mockBatchWrite;
function mockBatchGet(fn, returns, delay) {
    return mockCall("batchGet", fn, returns, false, delay);
}
exports.mockBatchGet = mockBatchGet;
function mockTransactGet(fn, returns, delay) {
    return mockCall("transactGet", fn, returns, false, delay);
}
exports.mockTransactGet = mockTransactGet;
function mockCall(name, fn, returns = {}, alwaysMock = false, delay) {
    return async () => {
        const spy = setupMock(name, returns, alwaysMock, delay);
        // TODO: Type cleanup
        const client = new aws_sdk_1.default.DynamoDB.DocumentClient();
        if (!mockOn && !alwaysMock) {
            // TODO: Type cleanup
            await fn(client, jest.spyOn(client, name).mock);
        }
        else {
            await fn(client, spy.mock);
        }
        teardownMock(name, alwaysMock);
    };
}
function setupMock(name, returns = {}, alwaysMock, delay) {
    const spy = jest.fn();
    let callCount = 0;
    if (mockOn || alwaysMock) {
        aws_sdk_mock_1.default.setSDKInstance(aws_sdk_1.default);
        aws_sdk_mock_1.default.mock("DynamoDB.DocumentClient", name, function (input, callback) {
            var _a, _b;
            spy(input);
            if (Array.isArray(returns)) {
                if (typeof delay === "number") {
                    setTimeout(() => {
                        var _a, _b;
                        callback((_a = returns[callCount]) === null || _a === void 0 ? void 0 : _a.err, (_b = returns[callCount]) === null || _b === void 0 ? void 0 : _b.data);
                        callCount += 1;
                    }, delay);
                }
                else {
                    callback((_a = returns[callCount]) === null || _a === void 0 ? void 0 : _a.err, (_b = returns[callCount]) === null || _b === void 0 ? void 0 : _b.data);
                    callCount += 1;
                }
            }
            else if (typeof delay === "number") {
                setTimeout(() => callback(returns === null || returns === void 0 ? void 0 : returns.err, returns === null || returns === void 0 ? void 0 : returns.data), delay);
            }
            else {
                callback(returns === null || returns === void 0 ? void 0 : returns.err, returns === null || returns === void 0 ? void 0 : returns.data);
            }
        });
    }
    return spy;
}
function teardownMock(name, alwaysMock) {
    if (mockOn || alwaysMock) {
        aws_sdk_mock_1.default.restore("DynamoDB.DocumentClient", name);
    }
}
