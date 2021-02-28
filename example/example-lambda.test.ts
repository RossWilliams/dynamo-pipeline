// import { mockUpdate, multiMock } from "dynamo-pipeline/testHelpers";
import { mockUpdate, multiMock } from "../src/testHelpers";
import { handler } from "./example-lambda";

const testEvent = {
  userId: "1",
  startDateTime: "2020-10-21T01:02:03.001Z",
  expectedVersion: 1,
};

describe("Example Lambda", () => {
  test(
    "Throws when invalid startDateTime event property is supplied",
    mockUpdate(
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
    mockUpdate(
      async (_client, spy) => {
        const result = await handler({ ...testEvent, expectedVersion: 0 });
        const request = spy.calls[0]![0]; // eslint-disable-line

        expect(spy.calls.length).toEqual(1);
        expect(request.ConditionExpression).toBeTruthy();
        expect(request.ExpressionAttributeValues?.[":v2"]).toEqual(0);
        expect("error" in result && result.error).toEqual("Version Conflict Error");
      },
      { data: { Attributes: { currentVersion: 2 } } }
    )
  );

  test(
    "When expected version is valid, queries existing calendar items in next 7 days",
    multiMock(
      async (_client, spies) => {
        const result = await handler(testEvent);
        // eslint-disable-next-line
        const querySpy = spies[1]!;

        expect("error" in result).toBeFalsy();
        expect(querySpy.calls.length).toEqual(1);
        const request = querySpy.calls[0]?.[0];
        expect(request.IndexName).toEqual("gsi1");
        expect(request.KeyConditionExpression?.includes("between")).toBeTruthy();
      },
      [
        { name: "update", returns: { data: { Attributes: { currentVersion: 2 } } } },
        { name: "query", returns: { data: { Items: [] } } },
        { name: "delete" },
        { name: "put" },
      ]
    )
  );

  test(
    "Deletes all existing queried calendar events",
    multiMock(
      async (_client, spies) => {
        const deleteSpy = spies[2]!; // eslint-disable-line

        await handler(testEvent);
        const deleteKeys = deleteSpy.calls.map((call) => call[0].Key);

        expect(deleteSpy.calls.length).toEqual(3);
        expect(deleteKeys[0]?.pk).toEqual("1");
        expect(deleteKeys[0]?.sk).toEqual("1");
        expect(deleteKeys[1]?.pk).toEqual("2");
        expect(deleteKeys[1]?.sk).toEqual("2");
        expect(deleteKeys[2]?.pk).toEqual("3");
        expect(deleteKeys[2]?.sk).toEqual("3");
      },
      [
        {
          name: "query",
          returns: {
            data: {
              Items: [
                { pk: "1", sk: "1" },
                { pk: "2", sk: "2" },
                { pk: "3", sk: "3" },
              ],
            },
          },
        },
        { name: "update", returns: { data: { Attributes: { currentVersion: 2 } } } },
        { name: "delete" },
        { name: "put" },
      ]
    )
  );

  test(
    "Adds new calendar item afer deleting other calendar items",
    multiMock(
      async (_queryClient, spies) => {
        const putSpy = spies[0]!; // eslint-disable-line

        await handler(testEvent);
        const request = putSpy.calls[0]![0]; // eslint-disable-line

        expect(putSpy.calls.length).toEqual(1);
        expect(request.Item.start).toEqual(testEvent.startDateTime);
        expect(request.Item.gsi1pk).toEqual(testEvent.userId);
      },
      [
        { name: "put" },
        {
          name: "query",
          returns: {
            data: {
              Items: [
                { pk: "1", sk: "1" },
                { pk: "2", sk: "2" },
                { pk: "3", sk: "3" },
              ],
            },
          },
        },
        { name: "delete" },
        { name: "update", returns: { data: { Attributes: { currentVersion: 2 } } } },
      ]
    )
  );
});
