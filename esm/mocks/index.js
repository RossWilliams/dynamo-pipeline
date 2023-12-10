import { DocumentClient } from "aws-sdk/clients/dynamodb";
import AWS from "aws-sdk";
import AWSMock from "aws-sdk-mock";
let mockOn = true;
export function setMockOn(on) {
    mockOn = on;
}
export function multiMock(fn, mockSet) {
    return async () => {
        const spies = mockSet.map((ms) => setupMock(ms.name, ms.returns, true, ms.delay).mock);
        const client = new DocumentClient();
        await fn(client, spies);
        mockSet.forEach((ms) => teardownMock(ms.name, true));
    };
}
export function mockScan(fn, returns, delay) {
    return mockCall("scan", fn, returns, false, delay);
}
export function alwaysMockScan(fn, returns, delay) {
    return mockCall("scan", fn, returns, true, delay);
}
export function mockQuery(fn, returns, delay) {
    return mockCall("query", fn, returns, false, delay);
}
export function alwaysMockQuery(fn, returns, delay) {
    return mockCall("query", fn, returns, true, delay);
}
export function alwaysMockBatchGet(fn, returns, delay) {
    return mockCall("batchGet", fn, returns, true, delay);
}
export function mockPut(fn, returns) {
    return mockCall("put", fn, returns);
}
export function mockUpdate(fn, returns) {
    return mockCall("update", fn, returns);
}
export function mockDelete(fn, returns) {
    return mockCall("delete", fn, returns);
}
export function alwaysMockBatchWrite(fn, returns) {
    return mockCall("batchWrite", fn, returns, true);
}
export function mockBatchWrite(fn, returns, delay) {
    return mockCall("batchWrite", fn, returns, false, delay);
}
export function mockBatchGet(fn, returns, delay) {
    return mockCall("batchGet", fn, returns, false, delay);
}
export function mockTransactGet(fn, returns, delay) {
    return mockCall("transactGet", fn, returns, false, delay);
}
function mockCall(name, fn, returns = {}, alwaysMock = false, delay) {
    return async () => {
        const spy = setupMock(name, returns, alwaysMock, delay);
        // TODO: Type cleanup
        // eslint-disable-next-line
        const client = new DocumentClient();
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
        AWSMock.setSDKInstance(AWS);
        AWSMock.mock("DynamoDB.DocumentClient", name, function (input, callback) {
            var _a, _b, _c, _d;
            spy(input);
            if (Array.isArray(returns)) {
                if (typeof delay === "number") {
                    setTimeout(() => {
                        var _a, _b, _c;
                        callback((_b = (_a = returns[callCount]) === null || _a === void 0 ? void 0 : _a.err) !== null && _b !== void 0 ? _b : undefined, (_c = returns[callCount]) === null || _c === void 0 ? void 0 : _c.data);
                        callCount += 1;
                    }, delay);
                }
                else {
                    callback((_b = (_a = returns[callCount]) === null || _a === void 0 ? void 0 : _a.err) !== null && _b !== void 0 ? _b : undefined, (_c = returns[callCount]) === null || _c === void 0 ? void 0 : _c.data);
                    callCount += 1;
                }
            }
            else if (typeof delay === "number") {
                setTimeout(() => { var _a; return callback((_a = returns === null || returns === void 0 ? void 0 : returns.err) !== null && _a !== void 0 ? _a : undefined, returns === null || returns === void 0 ? void 0 : returns.data); }, delay);
            }
            else {
                callback((_d = returns === null || returns === void 0 ? void 0 : returns.err) !== null && _d !== void 0 ? _d : undefined, returns === null || returns === void 0 ? void 0 : returns.data);
            }
        });
    }
    return spy;
}
function teardownMock(name, alwaysMock) {
    if (mockOn || alwaysMock) {
        AWSMock.restore("DynamoDB.DocumentClient", name);
    }
}
