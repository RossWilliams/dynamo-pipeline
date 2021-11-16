// import { mocks } from "dynamo-pipeline";;
import {
  DeleteCommand,
  PutCommand,
  UpdateCommand,
  QueryCommand,
  QueryCommandInput,
  DeleteCommandInput,
  PutCommandInput,
} from "@aws-sdk/lib-dynamodb";
import * as mocks from "../../lib/mocks";
import { handler } from "../src/example-lambda";

const testEvent = {
  userId: "1",
  startDateTime: "2020-10-21T01:02:03.001Z",
  expectedVersion: 1,
};

describe("Example Lambda", () => {
  test(
    "Throws when invalid startDateTime event property is supplied",
    mocks.mockUpdate(
      async (_client, spy) => {
        const result = await handler({ ...testEvent, startDateTime: "INVALID" });

        expect(spy.calls.length).toEqual(0);
        expect("error" in result && result.error).toEqual("Invalid startDateTime in Event");
      },
      { data: { Attributes: { currentVersion: 2 } } }
    )
  );

  test(
    "Attempts to update user profile, returns early if expected version does not match",
    mocks.multiMock(
      async (_client, spies) => {
        const result = await handler({ ...testEvent, expectedVersion: 0 });
        const request = spies[0]!.calls[0]![0].input; // eslint-disable-line

        expect(spies[0]!.calls.length).toEqual(1);
        expect(request.ConditionExpression).toBeTruthy();
        expect(request.ExpressionAttributeValues?.[":v2"]).toEqual(0);
        expect("error" in result && result.error).toEqual("Version Conflict Error");
      },
      [
        { name: UpdateCommand, returns: { data: { Attributes: { currentVersion: 2 } } } },
        { name: QueryCommand, returns: { data: { Items: [] } } },
        { name: DeleteCommand },
        { name: PutCommand },
      ]
    )
  );

  test(
    "When expected version is valid, queries existing calendar items in next 7 days",
    mocks.multiMock(
      async (_client, spies) => {
        const result = await handler(testEvent);
        // eslint-disable-next-line
        const querySpy = spies[1]!;

        expect("error" in result).toBeFalsy();
        expect(querySpy.calls.length).toEqual(1);
        const request = querySpy.calls[0]?.[0].input as QueryCommandInput;
        expect(request.IndexName).toEqual("gsi1");
        expect(request.KeyConditionExpression?.includes("BETWEEN")).toBeTruthy();
      },
      [
        { name: UpdateCommand, returns: { data: { Attributes: { currentVersion: 2 } } } },
        { name: QueryCommand, returns: { data: { Items: [] } } },
        { name: DeleteCommand },
        { name: PutCommand },
      ]
    )
  );

  test(
    "Deletes all existing queried calendar events",
    mocks.multiMock(
      async (_client, spies) => {
        const deleteSpy = spies[2]!; // eslint-disable-line

        await handler(testEvent);
        const deleteKeys = (deleteSpy.calls as [[{ input: DeleteCommandInput }]]).map((call) => call[0].input.Key);

        expect(deleteSpy.calls.length).toEqual(3);
        expect(deleteKeys[0]?.id).toEqual("1");
        expect(deleteKeys[0]?.sort).toEqual("1");
        expect(deleteKeys[1]?.id).toEqual("2");
        expect(deleteKeys[1]?.sort).toEqual("2");
        expect(deleteKeys[2]?.id).toEqual("3");
        expect(deleteKeys[2]?.sort).toEqual("3");
      },
      [
        {
          name: QueryCommand,
          returns: {
            data: {
              Items: [
                { id: "1", sort: "1" },
                { id: "2", sort: "2" },
                { id: "3", sort: "3" },
              ],
            },
          },
        },
        { name: UpdateCommand, returns: { data: { Attributes: { currentVersion: 2 } } } },
        { name: DeleteCommand },
        { name: PutCommand },
      ]
    )
  );

  test(
    "Adds new calendar item afer deleting other calendar items",
    mocks.multiMock(
      async (_queryClient, spies) => {
        const putSpy = spies[0]!; // eslint-disable-line

        await handler(testEvent);
        const request = putSpy.calls[0]![0].input as PutCommandInput; // eslint-disable-line

        expect(putSpy.calls.length).toEqual(1);
        expect(request.Item!.start).toEqual(testEvent.startDateTime);
        expect(request.Item!.gsi1pk).toEqual(testEvent.userId);
      },
      [
        { name: PutCommand },
        {
          name: QueryCommand,
          returns: {
            data: {
              Items: [
                { id: "1", sort: "1" },
                { id: "2", sort: "2" },
                { id: "3", sort: "3" },
              ],
            },
          },
        },
        { name: DeleteCommand },
        { name: UpdateCommand, returns: { data: { Attributes: { currentVersion: 2 } } } },
      ]
    )
  );
});
