import { Pipeline, sortKey } from "../src";
import {
  mockPut,
  setMockOn,
  mockUpdate,
  mockDelete,
  mockBatchWrite,
  alwaysMockBatchWrite,
  mockScan,
  alwaysMockScan,
  mockTransactGet,
  mockBatchGet,
  alwaysMockBatchGet,
  mockQuery,
  alwaysMockQuery,
} from "../src/mocks";
import { ensureDatabaseExists } from "./dynamodb.setup";

/*
When running against DynamoDB:
1. Ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION or AWS_DEFAULT_REGION set.
   1.1. Alternatively, ensure an ~/.aws/credentials file is set.
2. If assuming a role, ensure ~/.aws/config file is set, and AWS_PROFILE is set, and AWS_SDK_LOAD_CONFIG=1 is set.
3. Tests are kept to under 4,000 WCUs, can be run on newly created on-demand table.
*/
const TEST_WITH_DYNAMO = process.env.TEST_WITH_DYNAMO === "true" || process.env.TEST_WITH_DYNAMO === "1";
const TEST_TABLE = process.env.TEST_WITH_DYNAMO_TABLE || "dynamo-pipeline-e0558699b593";

describe("Dynamo Pipeline", () => {
  beforeAll(async () => {
    setMockOn(!TEST_WITH_DYNAMO);
    if (TEST_WITH_DYNAMO) {
      await ensureDatabaseExists(TEST_TABLE);
    }
    if (TEST_WITH_DYNAMO) {
      const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" });
      await pipeline.scan<{ id: string; sk: string }>().forEach((item) => pipeline.delete(item));
    }
  }, 30000);

  afterAll(async () => {
    if (TEST_WITH_DYNAMO) {
      const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" });
      await pipeline.scan<{ id: string; sk: string }>().forEach((item) => pipeline.delete(item));
    }
  });

  test("Creates a pipeline", () => {
    const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" });
    expect(pipeline).toBeDefined();
  });

  test("Updates pipeline config", () => {
    const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" })
      .withWriteBatchSize(22)
      .withWriteBuffer(13)
      .withReadBatchSize(99)
      .withReadBuffer(33);

    expect(pipeline.config.writeBatchSize).toEqual(22);
    expect(pipeline.config.writeBuffer).toEqual(13);
    expect(pipeline.config.readBatchSize).toEqual(99);
    expect(pipeline.config.readBuffer).toEqual(33);

    expect(() => pipeline.withWriteBatchSize(26)).toThrow();
    expect(() => pipeline.withWriteBuffer(-30)).toThrow();
    expect(() => pipeline.withReadBatchSize(0)).toThrow();

    const index = pipeline.createIndex("gsi1", { pk: "gsi1pk", sk: "gsi1sk" }).withReadBuffer(10).withReadBatchSize(34);

    expect(index.config.readBuffer).toEqual(10);
    expect(index.config.readBatchSize).toEqual(34);
    expect(() => index.withReadBuffer(-1)).toThrow();
    expect(() => index.withReadBatchSize(0)).toThrow();
  });

  describe("Put Item", () => {
    test(
      "Put item returns the same pipeline",
      mockPut(async (client, _spy) => {
        const pipeline = new Pipeline(
          TEST_TABLE,
          { pk: "id", sk: "sk" },
          {
            client,
          }
        );
        return expect(pipeline.put({ id: "put:1", sk: "1" })).resolves.toEqual(pipeline);
      })
    );

    test(
      "Failure to put adds item to unprocessed array",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          await pipeline.putIfNotExists({ id: "put:1", sk: "1", other: ["item", null], nullish: true });
          expect(pipeline.unprocessedItems.length).toEqual(1);
          const unprocessedItems = [];
          pipeline.handleUnprocessed((i) => unprocessedItems.push(i));
          expect(unprocessedItems.length).toEqual(1);
        },
        { err: new Error("item exists") }
      )
    );

    test(
      "Put sends a formatted put to the document client",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(
          TEST_TABLE,
          { pk: "id", sk: "sk" },
          {
            client,
          }
        );
        await pipeline.put({ id: "put:2", sk: "2" });

        const input = spy.calls[0]![0]; // eslint-disable-line

        expect(input?.Item).toStrictEqual({ id: "put:2", sk: "2" });
        expect(input?.ConditionExpression).not.toBeDefined();
        expect(input?.TableName).toEqual(TEST_TABLE);
      })
    );

    test(
      "Puts with attribute not exists condition",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(
          TEST_TABLE,
          { pk: "id", sk: "sk" },
          {
            client,
          }
        );
        await pipeline.put({ id: "put:1", sk: "1" }, { operator: "attribute_not_exists", property: "x" });
        const input = spy.calls[0]![0]; // eslint-disable-line
        expect(input?.Item).toStrictEqual({ id: "put:1", sk: "1" });
        expect(input?.ConditionExpression).toEqual("attribute_not_exists(#p0)");
        expect(input?.ExpressionAttributeNames).toStrictEqual({ "#p0": "x" });
        expect(input?.TableName).toEqual(TEST_TABLE);
      })
    );

    test(
      "Puts with intersect of conditions",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(
          TEST_TABLE,
          { pk: "id", sk: "sk" },
          {
            client,
          }
        );
        await pipeline.put(
          { id: "put:3", sk: "3" },
          {
            lhs: {
              lhs: { operator: "attribute_not_exists", property: "id" },
              logical: "OR",
              rhs: { operator: "attribute_exists", property: "xxx" },
            },
            logical: "AND",
            rhs: {
              logical: "NOT",
              rhs: { operator: "attribute_type", lhs: "other", rhs: { value: "S" } },
            },
          }
        );
        expect(pipeline.unprocessedItems.length).toEqual(0);
        const input = spy.calls[0]![0]; // eslint-disable-line
        expect(input?.ConditionExpression).toEqual(
          "(attribute_not_exists(#p0) OR attribute_exists(#p1)) AND (NOT attribute_type(#p2, :v0))"
        );
        expect(input?.ExpressionAttributeNames).toStrictEqual({
          "#p0": "id",
          "#p1": "xxx",
          "#p2": "other",
        });
        expect(input?.ExpressionAttributeValues).toStrictEqual({
          ":v0": "S",
        });
      })
    );

    test(
      "PutIfNotExists adds  pk condition",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.putIfNotExists({ id: "put:4", sk: "4", other: ["item"], nullish: null });
        const input = spy.calls[0]![0]; // eslint-disable-line
        expect(Object.keys(input.ExpressionAttributeNames || {}).length).toEqual(1);
        expect(Object.values(input.ExpressionAttributeNames || {})).toStrictEqual(["id"]);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Put with conditions can check if just an sk exists",
      mockPut(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          await pipeline.put({ id: "put:4", sk: "4" }, { operator: "attribute_not_exists", property: "sk" });
          const input = spy.calls[0]![0]; // eslint-disable-line

          expect(input.ExpressionAttributeValues).not.toBeDefined();
          expect(input.ExpressionAttributeNames).toStrictEqual({
            "#p0": "sk",
          });
          expect(input.ConditionExpression).toEqual("attribute_not_exists(#p0)");
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "Put with conditions can check value operators as valid",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.put({ id: "put:4", sk: "4" }, { lhs: "sk", operator: "<", rhs: { value: "5" } });

        const input = spy.calls[0]![0]; // eslint-disable-line
        expect(input.ExpressionAttributeValues).toStrictEqual({ ":v0": "5" });
        expect(input.ExpressionAttributeNames).toStrictEqual({ "#p0": "sk" });
        expect(input.ConditionExpression).toEqual("#p0 < :v0");
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Put with conditions can check value operators as invalid",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          await pipeline.put({ id: "put:4", sk: "4" }, { lhs: "sk", operator: ">", rhs: { value: "5" } });

          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "Put with conditions can check BETWEEN operator as valid",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.put({ id: "put:6", sk: "6" });

        await pipeline.put(
          { id: "put:6", sk: "6", other: 1 },
          { operator: "between", property: "sk", start: "5", end: "7" }
        );

        expect(pipeline.unprocessedItems.length).toEqual(0);

        await pipeline.put(
          { id: "put:6", sk: "6", other: 2 },
          { operator: "between", property: "other", start: 1, end: 3 }
        );

        expect(pipeline.unprocessedItems.length).toEqual(0);

        const input = spy.calls[2]![0]; // eslint-disable-line

        expect(input.ExpressionAttributeValues).toStrictEqual({
          ":v0": 1,
          ":v1": 3,
        });
        expect(input.ExpressionAttributeNames).toStrictEqual({
          "#p0": "other",
        });
        expect(input.ConditionExpression).toEqual("#p0 BETWEEN :v0 AND :v1");
      })
    );

    test(
      "Put with conditions can check BETWEEN operator as invalid",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          let unprocessedCount = 0;

          await pipeline.put({ id: "put:6", sk: "6", other: 3 });
          unprocessedCount += pipeline.unprocessedItems.length > unprocessedCount ? 1 : 0;

          await pipeline.put(
            { id: "put:6", sk: "6", other: 3 },
            { operator: "between", property: "other", start: 4, end: 5 }
          );

          expect(pipeline.unprocessedItems.length).toEqual(unprocessedCount + 1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "Put with conditions can check IN operator as valid",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.put({ id: "put:7", sk: "7" });

        await pipeline.put(
          { id: "put:7", sk: "7", other: 1 },
          { operator: "in", property: "sk", list: ["4", "5", "6", "7"] }
        );

        expect(pipeline.unprocessedItems.length).toEqual(0);

        await pipeline.put(
          { id: "put:7", sk: "7", other: 2 },
          {
            operator: "in",
            property: "other",
            list: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
          }
        );

        expect(pipeline.unprocessedItems.length).toEqual(0);

        // eslint-disable-next-line
        const input = spy.calls[1]![0];

        expect(input.ExpressionAttributeValues).toStrictEqual({
          ":v0": "4",
          ":v1": "5",
          ":v2": "6",
          ":v3": "7",
        });
        expect(input.ExpressionAttributeNames).toStrictEqual({
          "#p0": "sk",
        });
        expect(input.ConditionExpression).toEqual("#p0 IN (:v0,:v1,:v2,:v3)");
      })
    );

    test(
      "Put with conditions can check IN as invalid",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          let unprocessedCount = 0;

          await pipeline.put({ id: "put:7", sk: "7", other: 3 });
          unprocessedCount += pipeline.unprocessedItems.length > unprocessedCount ? 1 : 0;

          await pipeline.put(
            { id: "put:7", sk: "7", other: 4 },
            { property: "other", operator: "in", list: [4, 5, 6, 7, 8, 9, 10] }
          );

          expect(pipeline.unprocessedItems.length).toEqual(unprocessedCount + 1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "PutItems with more than 25 items batches into multiple writes",
      mockBatchWrite(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          // On-Demand tables start with capacity of 4000 WCU, use 6000 WCUs to stress system
          const items = new Array(6000).fill(0).map((_, i) => ({
            id: "putMany:6000",
            sk: i.toString(),
            other: new Array(6)
              .fill(0)
              .map(() => Math.random().toString(36).substring(2, 15))
              .join(""),
          }));

          // set to 60 to trigger backoff
          await pipeline.putItems(items, { bufferCapacity: 60 });
          if (TEST_WITH_DYNAMO) {
            const inserted = await pipeline.query({ id: "putMany:6000" }, { consistentRead: true }).all();
            expect(inserted.length).toEqual(6000);
          }

          expect(pipeline.unprocessedItems.length).toEqual(0);
        },
        [],
        20
      ),
      30000
    );

    test("PutItems with invalid buffers throws", async () => {
      const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" });
      await expect(pipeline.putItems([{ id: "1", sk: "1" }], { bufferCapacity: -1 })).rejects.toThrow();
      await expect(pipeline.putItems([{ id: "1", sk: "1" }], { batchSize: 0 })).rejects.toThrow();
      await expect(pipeline.putItems([{ id: "1", sk: "1" }], { batchSize: 26 })).rejects.toThrow();
    });

    test(
      "PutItems with invalid item returns the invalid chunk.",
      alwaysMockBatchWrite(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          const items: any[] = new Array(53).fill(0).map((_, i) => ({
            id: "putMany:" + i.toString(),
            sk: i.toString(),
            other: new Array(6)
              .fill(0)
              .map(() => Math.random().toString(36).substring(2, 15))
              .join(""),
          }));
          items.push({ id: "putMany:bad", other: 1 });
          items.push({ id: "putMany:bad2", other: 1 });
          const result = pipeline.putItems(items);

          try {
            await result;
          } catch {}

          await expect(result).rejects.toBeDefined();
          expect(pipeline.unprocessedItems.length).toEqual(5);
        },
        [{ data: {} }, { data: {} }, { err: Error("err") }]
      )
    );

    test(
      "PutItems with processing error returns unprocessed item.",
      alwaysMockBatchWrite(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          const items = [
            { id: "putMany:good", sk: "2", other: 1 },
            { id: "putMany:bad2", sk: "2", other: 1 },
          ];
          await pipeline.putItems(items);
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        [
          {
            data: {
              UnprocessedItems: {
                [TEST_TABLE]: [{ PutRequest: { Item: { id: "putMany:bad2", sk: "2", other: 1 } } }],
              },
            },
          },
          {
            data: {
              UnprocessedItems: {
                [TEST_TABLE]: [{ PutRequest: { Item: { id: "putMany:bad2", sk: "2", other: 1 } } }],
              },
            },
          },
        ]
      )
    );
  });

  describe("Update Item", () => {
    beforeAll(
      mockPut(async (client, _spy) => {
        const pipeline = new Pipeline(
          TEST_TABLE,
          { pk: "id", sk: "sk" },
          {
            client,
          }
        );

        await pipeline.put({ id: "update:1", sk: "1", other: 1 });
      })
    );

    test(
      "Update item returns new or old values",
      mockUpdate(
        async (client, _spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );

          const result = await pipeline.update(
            { id: "update:1", sk: "1" },
            { other: 2 },
            { returnType: "UPDATED_NEW" }
          );
          expect(result).toStrictEqual({ other: 2 });

          const result2 = await pipeline.update({ id: "update:1", sk: "1" }, { other: 3 });

          expect(result2).toStrictEqual(null);
        },
        [{ data: { Attributes: { other: 2 } } }, { data: undefined }]
      )
    );

    test(
      "Update item with valid condition updates item",
      mockUpdate(
        async (client, spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          const result = await pipeline.update(
            { id: "update:1", sk: "1" },
            { other: 4 },
            {
              condition: {
                lhs: "other",
                operator: ">",
                rhs: { value: 0 },
              },
              returnType: "UPDATED_NEW",
            }
          );

          const input = spy.calls[0]?.[0] || ({} as Record<string, any>); // eslint-disable-line
          expect(result).toStrictEqual({ other: 4 });
          expect(input.UpdateExpression).toEqual("SET #other = :other");
          expect(input.ConditionExpression).toEqual("#p1 > :v1");
          expect(input.ExpressionAttributeNames).toStrictEqual({
            "#p1": "other",
            "#other": "other",
          });
          expect(input.ExpressionAttributeValues).toStrictEqual({ ":other": 4, ":v1": 0 });
        },
        { data: { Attributes: { other: 4 } } }
      )
    );

    test(
      "Update item with undefined attribute removes the item",
      mockUpdate(
        async (client, spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          const result = await pipeline.update({ id: "update:1", sk: "1" }, { other: undefined, new: 2 });

          const input = spy.calls[0]?.[0] || ({} as Record<string, any>); // eslint-disable-line
          expect(result).toStrictEqual({ new: 2 });
          expect(input.UpdateExpression).toEqual("SET #new = :new, REMOVE #other");
          expect(input.ExpressionAttributeNames).toStrictEqual({
            "#other": "other",
            "#new": "new",
          });
          expect(input.ExpressionAttributeValues).toStrictEqual({ ":new": 2 });
        },
        { data: { Attributes: { new: 2 } } }
      )
    );

    test(
      "Update item with invalid condition does not update item",
      mockUpdate(
        async (client, spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          const result = await pipeline.update(
            { id: "update:1", sk: "1" },
            { other: 5 },
            {
              condition: {
                lhs: "other",
                operator: "<",
                rhs: { value: 0 },
              },
              returnType: "UPDATED_NEW",
            }
          );

          const input = spy.calls[0]?.[0] || { ConditionExpression: null }; // eslint-disable-line
          expect(result).toEqual(null);
          expect(input.ConditionExpression).toEqual("#p1 < :v1");
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );
  });

  describe("Delete Item", () => {
    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(
          TEST_TABLE,
          { pk: "id", sk: "sk" },
          {
            client,
          }
        );

        await pipeline.putItems([
          { id: "delete:1", sk: "1", other: 1 },
          { id: "delete:2", sk: "2", other: 2 },
          { id: "delete:3", sk: "3", other: 3 },
        ]);

        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Delete item returns old values",
      mockDelete(
        async (client, _spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );

          const result = await pipeline.delete({ id: "delete:1", sk: "1" }, { returnType: "ALL_OLD" });
          expect(result).toStrictEqual({ other: 1, id: "delete:1", sk: "1" });
        },
        { data: { Attributes: { other: 1, id: "delete:1", sk: "1" } } }
      )
    );

    test(
      "Delete item with valid condition deletes item",
      mockDelete(
        async (client, spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          const result = await pipeline.delete(
            { id: "delete:2", sk: "2" },
            { condition: { lhs: "other", operator: ">", rhs: { value: 0 } }, returnType: "ALL_OLD" }
          );

          const input = spy.calls[0]![0]; // eslint-disable-line
          expect(result).toStrictEqual({ other: 2, id: "delete:2", sk: "2" });
          expect(input.ConditionExpression).toEqual("#p0 > :v0");
          expect(input.ExpressionAttributeNames).toStrictEqual({
            "#p0": "other",
          });
          expect(input.ExpressionAttributeValues).toStrictEqual({ ":v0": 0 });
        },
        { data: { Attributes: { other: 2, id: "delete:2", sk: "2" } } }
      )
    );

    test(
      "Delete item with invalid condition does not delete item",
      mockDelete(
        async (client, spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          const result = await pipeline.delete(
            { id: "delete:3", sk: "3" },
            { condition: { lhs: "other", operator: "<", rhs: { value: 0 } }, reportError: true }
          );

          const input = spy.calls[0]![0]; // eslint-disable-line
          expect(result).toEqual(null);
          expect(input.ConditionExpression).toEqual("#p0 < :v0");
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );

    // note by default we set a condition to error if the item does not exist.
    test(
      "Delete item not in db adds to unprocessed list",
      mockDelete(
        async (client, _spy) => {
          const pipeline = new Pipeline(
            TEST_TABLE,
            { pk: "id", sk: "sk" },
            {
              client,
            }
          );
          const result = await pipeline.delete({ id: "delete:3", sk: "4" }, { reportError: true });
          expect(result).toEqual(null);
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );
  });

  describe("Scan", () => {
    const items = new Array(100).fill(0).map((_, i) => ({
      id: "scan:" + i.toString(),

      sk: i.toString(),
      ...((i === 1 || i === 2) && {
        gsi1pk: "scanIndex:1",
        gsi1sk: (i + 1).toString(),
      }),
      plusOne: (i + 1).toString(),
      other: new Array(1200)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.putItems(items);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Scan Index will get all items projected onto the index",
      mockScan(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const index = pipeline.createIndex("gsi1", { pk: "gsi1pk", sk: "gsi1sk" });

          const results = await index.scan().all();
          expect(results.length).toEqual(2);
        },
        [{ data: { Items: items.slice(1, 3) } }]
      )
    );

    test(
      "Scan will fetch multiple times to get all items",
      mockScan(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline.scan<Data>({
            batchSize: 50,
            filters: { property: "id", operator: "begins_with", value: "scan:" },
          });

          const result = await scanner.all();
          expect(result.length).toEqual(100);
        },
        [{ data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } }, { data: { Items: items.slice(50, 100) } }]
      )
    );

    test(
      "Scan will limit the amount of items returned",
      mockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline.scan<Data>({
            batchSize: 50,
            limit: 80,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });

          let nextToken: Record<string, unknown> | undefined;

          const result: Data[] = await scanner.onLastEvaluatedKey((i) => (nextToken = i)).all();
          expect(result.length).toEqual(80);
          if (!TEST_WITH_DYNAMO) {
            // filter makes dynamo target unstable in number of calls
            expect(spy.calls.length).toEqual(2);
          }

          expect(nextToken).toBeDefined();
          expect(nextToken?.id).toBeDefined();
          expect(nextToken?.sk).toBeDefined();
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 90), LastEvaluatedKey: { id: "1", sk: "2" } } },
          { data: { Items: items.slice(90, 100) } },
        ]
      )
    );

    test(
      "Scan will fetch a second time before the user starts to process content",
      mockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline.scan<Data>({
            batchSize: 50,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });

          let processing = false;
          await scanner.forEach(() => {
            if (!processing && !TEST_WITH_DYNAMO) {
              expect(spy.calls.length).toEqual(2);
            }
            processing = true;
          });

          expect(spy.calls.length).toEqual(2);
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 100) } },
        ],
        1
      )
    );

    test(
      "Scan will batch results to allow pausing to fetch more",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client, readBuffer: 1 });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline.scan<Data>({
            batchSize: 10,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });

          let checked = false;
          let index = 0;
          await scanner.forEach(() => {
            if (!checked && index === 45) {
              expect(spy.calls.length).toEqual(2);
              checked = true;
            }
            index += 1;
          });

          expect(spy.calls.length).toEqual(4);
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 70), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(70, 90), LastEvaluatedKey: { N: 100 } } },
          { data: { Items: items.slice(90, 100) } },
        ]
      )
    );

    test(
      "Scan will not exceed the configuration buffer limit",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client, readBuffer: 1 });
          type Data = { id: string; sk: string; other: string };
          const result: Data[] = [];
          const scanner = pipeline.scan<Data>({
            batchSize: 5,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });
          await scanner.forEach((item) => {
            result.push(item);
            // a single response is 10 items, so a read buffer of 1 will still buffer 10 items.
            expect((spy.calls.length - 1) * 10 - result.length).toBeLessThanOrEqual(10);

            return new Promise<void>((resolve) => setImmediate(() => resolve(), 5));
          });

          expect(result.length).toEqual(100);
        },
        [
          { data: { Items: items.slice(0, 10), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(10, 20), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(20, 30), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(30, 40), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(40, 50), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(50, 60), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(60, 70), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(70, 80), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(80, 90), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(90, 100), LastEvaluatedKey: { N: 10 } } },
        ],
        1
      )
    );

    test(
      "Scan with filter returning empty results array does not cause fetcher to stall.",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client, readBuffer: 1 });
          type Data = { id: string; sk: string; other: string };

          const scanner = pipeline.scan<Data>({
            batchSize: 5,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });
          const result = await scanner.all();

          expect(result.length).toEqual(100);
        },
        [
          { data: { Items: items.slice(0, 20), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(20, 40), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(40, 60), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: [], LastEvaluatedKey: { N: 4 } } },
          { data: { Items: [], LastEvaluatedKey: { N: 5 } } },
          { data: { Items: [], LastEvaluatedKey: { N: 6 } } },
          { data: { Items: [], LastEvaluatedKey: { N: 7 } } },
          { data: { Items: [], LastEvaluatedKey: { N: 8 } } },
          { data: { Items: [], LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(60, 80), LastEvaluatedKey: { N: 10 } } },
          { data: { Items: items.slice(80, 100) } },
        ],
        1
      )
    );

    test(
      "Scan will not buffer additional items if read buffer set to zero",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client, readBuffer: 0 });
          type Data = { id: string; sk: string; other: string };

          const scanner = pipeline.scan<Data>({
            batchSize: 5,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });

          const result: Data[] = await scanner.map((i, index) => {
            // a single response is 10 items, so a read buffer of 0 will still buffer 10 items.
            expect(spy.calls.length * 10 - index).toBeLessThanOrEqual(10 + 5);

            return i;
          });

          expect(result.length).toEqual(100);
        },
        [
          { data: { Items: items.slice(0, 10), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(10, 20), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(20, 30), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(30, 40), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(40, 50), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(50, 60), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(60, 70), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(70, 80), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(80, 90), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(90, 100) } },
        ],
        1
      )
    );

    test(
      "Scan will not exceed the configuration buffer limit with high buffer limit",
      alwaysMockScan(
        async (client, spy) => {
          const readBuffer = 5;
          const batchSize = 10;
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client, readBuffer });
          type Data = { id: string; sk: string; other: string };
          const result: Data[] = [];
          const scanner = pipeline.scan<Data>({
            batchSize,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });
          await scanner.forEach((item) => {
            result.push(item);
            // find the buffered items, subtract out processed items, should be less than the max buffer.
            expect((spy.calls.length - 1) * 10 - result.length).toBeLessThanOrEqual(batchSize * readBuffer + batchSize);

            return new Promise<void>((resolve) => setImmediate(() => resolve(), 5));
          });

          expect(result.length).toEqual(100);
        },
        [
          { data: { Items: items.slice(0, 10), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(10, 20), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(20, 30), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(30, 40), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(40, 50), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(50, 60), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(60, 70), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(70, 80), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(80, 90), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(90, 100) } },
        ],
        1
      )
    );

    test(
      "Scan processing in a forEach which calls cancel stops processing and fetching",
      alwaysMockScan(
        async (client, spy) => {
          const readBuffer = 5;
          const batchSize = 10;
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client, readBuffer });
          type Data = { id: string; sk: string; other: string };
          const result: Data[] = [];
          const scanner = pipeline.scan<Data>({
            batchSize,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });
          await scanner.forEach((item, _index, _pipeline, cancel) => {
            result.push(item);
            if (result.length === 8) {
              cancel();
              return;
            }
            // find the buffered items, subtract out processed items, should be less than the max buffer.
            return new Promise<void>((resolve) => setImmediate(() => resolve(), 5));
          });

          expect(spy.calls.length).toEqual(2);
          expect(result.length).toEqual(8);
        },
        [
          { data: { Items: items.slice(0, 10), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(10, 20), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(20, 30), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(30, 40), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(40, 50), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(50, 60), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(60, 70), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(70, 80), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(80, 90), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(90, 100) } },
        ],
        1
      )
    );

    test(
      "Using forEach will give the correct index number for each item returned",
      mockScan(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline.scan<Data>({
            batchSize: 50,
            filters: {
              property: "id",
              operator: "begins_with",
              value: "scan:",
            },
          });

          const seenIndexes: boolean[] = Array<boolean>(100).fill(false);

          await scanner.forEach((_item, index) => {
            seenIndexes[index] = true;
          });

          expect(seenIndexes.every((i) => i)).toEqual(true);
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 100) } },
        ],
        1
      )
    );
  });

  describe("Get Items", () => {
    const items = new Array<number>(150).fill(0).map((_, i) => ({
      id: "getItems:" + i.toString(),
      sk: i.toString(),
      other: new Array(6)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.putItems(items);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test("Get with batch size over 100 throws", () => {
      const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" });
      expect(() => pipeline.getItems([], { batchSize: 125 })).toThrow();
    });

    test("Get with buffer capacity under 0 throws", () => {
      const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" });
      expect(() => pipeline.getItems([], { bufferCapacity: -1 })).toThrow();
    });

    test(
      "Get 2 items returns both items",
      mockBatchGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const result = await pipeline
            .getItems<{ other: string }>([
              { id: "getItems:1", sk: "1" },
              { id: "getItems:2", sk: "2" },
            ])
            .all();

          expect(result.length).toEqual(2);
          expect(result[0]?.other).toBeTruthy();
        },
        { data: { Responses: { [TEST_TABLE]: items.slice(0, 2) } } }
      )
    );

    test(
      "Get 150 items returns all items",
      mockBatchGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(4);
          const results = await pipeline.getItems<{ other: string }>(items.slice(0, 150)).all();

          expect(results.length).toEqual(150);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 100) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(100, 150) } } },
        ]
      )
    );

    test(
      "Get with unprocessed keys retries the request across two requests",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(4);

          const all = await pipeline
            .getItems<{ other: string }>(items.slice(0, 100), { batchSize: 20 })
            .all();

          expect(all.length).toEqual(100);
          expect(pipeline.unprocessedItems.length).toEqual(0);
          expect(spy.calls.length).toEqual(7);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 20) } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(20, 40) } } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(40, 60) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(60, 80) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(80, 100) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(20, 30) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(30, 40) } } },
        ]
      )
    );

    test(
      "Get with failed retry add the item to the unprocessed items list",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(4);

          const all = await pipeline
            .getItems<{ other: string }>(items.slice(0, 100), { batchSize: 20 })
            .all();

          expect(all.length).toEqual(80);
          expect(pipeline.unprocessedItems.length).toEqual(20);
          expect(spy.calls.length).toEqual(7);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 20) } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(20, 40) } } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(40, 60) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(60, 80) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(80, 100) } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(20, 30) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(30, 40) } } } },
        ]
      )
    );

    test(
      "Get with partial unprocessed keys retries the request across two requests",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(2);

          const all = await pipeline
            .getItems<{ other: string }>(items.slice(0, 100), { batchSize: 20 })
            .all();

          expect(all.length).toEqual(100);
          expect(pipeline.unprocessedItems.length).toEqual(0);
          expect(spy.calls.length).toEqual(7);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 20) } } },
          {
            data: {
              Responses: { [TEST_TABLE]: items.slice(20, 30) },
              UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(30, 40) } },
            },
          },
          { data: { Responses: { [TEST_TABLE]: items.slice(40, 60) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(60, 80) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(80, 100) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(30, 35) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(35, 40) } } },
        ]
      )
    );

    test(
      "Get with unprocessed keys that fail retry are set as unhandled for the user to deal with",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(2);

          const all = await pipeline
            .getItems<{ other: string }>(items.slice(0, 100), { batchSize: 20 })
            .all();

          expect(all.length).toEqual(98);
          expect(pipeline.unprocessedItems.length).toEqual(2);
          expect(spy.calls.length).toEqual(7);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 20) } } },
          {
            data: {
              Responses: { [TEST_TABLE]: items.slice(20, 30) },
              UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(30, 40) } },
            },
          },
          { data: { Responses: { [TEST_TABLE]: items.slice(40, 60) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(60, 80) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(80, 100) } } },
          {
            data: {
              Responses: { [TEST_TABLE]: items.slice(30, 33) },
              UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(33, 35) } },
            },
          },
          { data: { Responses: { [TEST_TABLE]: items.slice(35, 40) } } },
        ]
      )
    );

    test(
      "Get respects set read buffer",
      mockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(2);
          let index = 0;
          await pipeline
            .getItems<{ other: string }>(items.slice(0, 100), { batchSize: 20 })
            .forEach(() => {
              if (index === 39) {
                expect(spy.calls.length).toEqual(4);
              } else if (index === 40) {
                expect(spy.calls.length).toEqual(5);
              }
              index += 1;
            });

          expect(spy.calls.length).toEqual(5);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 20) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(20, 40) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(40, 60) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(60, 80) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(80, 100) } } },
        ],
        10
      )
    );

    test(
      "Get can return many empty results without stalling pipeline",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(2);
          const result = await pipeline
            .getItems<{ other: string }>(items.slice(0, 120), { batchSize: 20 })
            .all();
          expect(spy.calls.length).toEqual(14);
          expect(result.length).toEqual(40);
          expect(pipeline.unprocessedItems.length).toEqual(80);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 20) } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(20, 40) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(40, 60) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(60, 80) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(80, 100) } } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(100, 120) } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(20, 30) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(30, 40) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(40, 50) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(50, 60) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(60, 70) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(70, 80) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(80, 90) } } } },
          { data: { UnprocessedKeys: { [TEST_TABLE]: { Keys: items.slice(90, 100) } } } },
        ],
        10
      )
    );

    test(
      "Get handles receiving an empty response",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBuffer(2);

          const result = await pipeline
            .getItems<{ other: string }>(items.slice(0, 20), { batchSize: 20 })
            .all();

          expect(spy.calls.length).toEqual(1);
          expect(result.length).toEqual(0);
        },
        [{ data: { Responses: { [TEST_TABLE]: [] } } }, { data: { Responses: {} } }],
        10
      )
    );
  });

  describe("Transact Get", () => {
    const items = new Array(5).fill(0).map((_, i) => ({
      id: "transactGet:" + i.toString(),
      sk: i.toString(),
      other: new Array<number>(6)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.putItems(items);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Transact Get 2 items returns both items",
      mockTransactGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const result = await pipeline
            .transactGet<{ other: string }>([
              { id: "transactGet:1", sk: "1" },
              { id: "transactGet:2", sk: "2" },
            ])
            .all();
          expect(result.length).toEqual(2);
          expect(result[0]?.other).toBeTruthy();
        },
        { data: { Responses: [{ Item: items[0] }, { Item: items[1] }] } }
      )
    );

    test(
      "Transact Get 2 items where one item doesn't exist returns 1 item",
      mockTransactGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const result = await pipeline
            .transactGet<{ other: string }>([
              { id: "transactGet:1", sk: "1" },
              { id: "transactGet:-1", sk: "2" },
            ])
            .all();
          expect(result.length).toEqual(1);
        },
        { data: { Responses: [{ Item: items[0] }] } }
      )
    );

    test(
      "Transact Get 2 items can specify the name of the table for each item",
      mockTransactGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const result = await pipeline
            .transactGet<{ other: string }>([
              { tableName: TEST_TABLE, keys: { id: "transactGet:1", sk: "1" }, keyDefinition: { pk: "id", sk: "sk" } },
              { tableName: TEST_TABLE, keys: { id: "transactGet:2", sk: "2" }, keyDefinition: { pk: "id", sk: "sk" } },
            ])
            .all();
          expect(result.length).toEqual(2);
        },
        { data: { Responses: [{ Item: items[0] }, { Item: items[1] }] } }
      )
    );
  });

  describe("Query", () => {
    const items = new Array(100).fill(0).map((_, i) => ({
      id: "query:1",
      sk: i.toString(),
      gsi1pk: "queryIndex:1",
      gsi1sk: (i + 1).toString(),
      plusOne: (i + 1).toString(),
      evenIsOne: i % 2 === 0 ? 1 : 0,
      other: new Array(250)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    const items2 = new Array(5).fill(0).map((_, i) => ({
      id: "query:2",
      sk: i.toString(),
      plusOne: (i + 1).toString(),
      evenIsOne: i % 2 === 0 ? 1 : 0,
      other: new Array(5)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    const items3 = new Array(5000).fill(0).map((_, i) => ({
      id: "query:3",
      sk: i.toString(),
      gsi1pk: "1",
      gsi1sk: "1",
      other: new Array(50)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

        await pipeline.putItems(items);
        await pipeline.putItems(items2);
        await pipeline.putItems(items3);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Query can use a gsi with more than 1MB of the same pk and sk",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const result = await pipeline.createIndex("gsi1", { pk: "gsi1pk", sk: "gsisk" }).query({ gsi1pk: "1" }).all();
          expect(result.length).toEqual(5000);
        },
        [
          { data: { Items: items3.slice(0, 2500), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items3.slice(2500, 5000) } },
        ]
      )
    );

    test(
      "Query can return no results without throwing",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const result = await pipeline.query({ id: "query:xxx" }).all();
          expect(result.length).toEqual(0);
        },
        [{ data: { Items: [] } }]
      )
    );

    test(
      "Query will fetch multiple times to get all items",
      mockQuery(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const query = pipeline.query<Data>(
            { id: "query:1" },
            {
              batchSize: 100,
              consistentRead: true,
            }
          );

          const result = await query.all();
          expect(result.length).toEqual(100);
        },
        [{ data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } }, { data: { Items: items.slice(50, 100) } }]
      )
    );

    test(
      "Query can use begins_with operator",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const query = pipeline.query<Data>(
            { id: "query:1", sk: sortKey("begins_with", "1") },
            {
              batchSize: 100,
              consistentRead: true,
            }
          );

          const result = await query.all();
          const request = spy.calls[0]![0]; // eslint-disable-line
          expect(request.KeyConditionExpression?.indexOf("begins_with(")).toBeGreaterThan(-1);
          expect(result.length).toEqual(11);
        },
        [{ data: { Items: items.slice(0, 11), LastEvaluatedKey: { N: 1 } } }]
      )
    );

    test(
      "Query can use BETWEEN operator",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };
          const query = pipeline.query<Data>(
            { id: "query:1", sk: sortKey("between", "1", "2") },
            {
              batchSize: 100,
              consistentRead: true,
            }
          );

          const result = await query.all();
          const request = spy.calls[0]![0]; // eslint-disable-line
          expect(request.KeyConditionExpression?.indexOf("BETWEEN :v1 AND :v2")).toBeGreaterThan(-1);
          expect(result.length).toEqual(12);
        },
        [{ data: { Items: items.slice(0, 12), LastEvaluatedKey: { N: 1 } } }]
      )
    );

    test(
      "Query filtering can have properties on both sides of operator",
      mockQuery(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          type Data = { id: string; sk: string; other: string };

          const query = pipeline.query<Data>(
            { id: "query:1" },
            {
              batchSize: 50,
              consistentRead: true,
              filters: {
                lhs: { lhs: "evenIsOne", operator: "=", rhs: { value: 1 } },
                logical: "AND",
                rhs: {
                  lhs: {
                    lhs: "plusOne",
                    operator: ">",
                    rhs: {
                      value: "4",
                    },
                  },
                  logical: "OR",
                  rhs: {
                    lhs: {
                      lhs: {
                        function: "size" as const,
                        property: "other",
                      },
                      operator: ">",
                      rhs: { value: 42 },
                    },
                    logical: "AND",
                    rhs: {
                      lhs: "gsi1sk",
                      operator: "<",
                      rhs: {
                        property: "plusOne",
                      },
                    },
                  },
                },
              },
            }
          );

          const result = await query.all();
          expect(result.length).toEqual(33);
        },
        [
          {
            data: {
              Items: items
                .slice(0, 50)
                .filter((_v, i) => i % 2 === 0)
                .filter((v) => v.plusOne > "4" || v.gsi1sk < v.plusOne),
              LastEvaluatedKey: { N: 1 },
            },
          },
          { data: { Items: items.slice(50, 100).filter((_v, i) => i % 2 === 0) } },
        ]
      )
    );

    test(
      "Query will limit the amount of items returned",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const query = pipeline.query(
            { id: "query:1" },
            {
              batchSize: 50,
              limit: 80,
              // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
              nextToken: { id: items[19]!.id, sk: items[19]!.sk },
              consistentRead: true,
              filters: {
                lhs: {
                  operator: "attribute_exists",
                  property: "other",
                },
                logical: "AND",
                rhs: {
                  property: "evenIsOne",
                  operator: "in",
                  list: [0, 1, 2],
                },
              },
            }
          );

          const result: any[] = await query.all();
          expect(result.length).toEqual(80);
          expect(spy.calls.length).toEqual(2);
        },
        [
          { data: { Items: items.slice(20, 70), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(70, 100) } },
        ]
      )
    );

    test(
      "Query does not need to supply options",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const query = pipeline.query({ id: "query:1" });

          const result: any[] = await query.all();
          expect(result.length).toEqual(100);
          if (!TEST_WITH_DYNAMO) {
            expect(spy.calls.length).toEqual(2);
          }
        },
        [{ data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } }, { data: { Items: items.slice(50, 100) } }]
      )
    );

    test(
      "Query can use less than operator to receive a subselection",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const query = pipeline.query({ id: "query:1", sk: sortKey("between", "1", "2") });

          const result: any[] = await query.all();
          expect(result.length).toEqual(12);
          expect(spy.calls.length).toEqual(1);
        },
        [{ data: { Items: items.slice(0, 2).concat(items.slice(10, 20)) } }]
      )
    );

    test(
      "Query can sort results descending",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const query = pipeline.query<{ id: string; sk: string }>(
            { id: "query:1" },
            { limit: 50, sortDescending: true }
          );

          const result = await query.all();
          expect(result.length).toEqual(50);
          expect(spy.calls.length).toEqual(1);
          expect(result.find((r) => r.sk === "48")).not.toBeDefined();
        },
        [{ data: { Items: items.slice(50, 100).concat([]).reverse() } }]
      )
    );

    test(
      "Query can use between operator to receive a subselection",
      mockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const query = pipeline.query({ id: "query:1", sk: sortKey("between", "2", "4") });

          const result: any[] = await query.all();
          expect(result.length).toEqual(23);
          expect(spy.calls.length).toEqual(1);
        },
        [{ data: { Items: items.slice(2, 5).concat(items.slice(20, 40)) } }]
      )
    );

    test(
      "Query AWS failure for any reason logs and throws",
      alwaysMockQuery(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          await expect(pipeline.query({ id: "query:1" }).all()).rejects.toBeDefined();
          await expect(pipeline.query({ id: "query:1" }, { limit: 100 }).all()).rejects.toBeDefined();
        },
        [
          { err: new Error("An AWS Error") },
          { data: { Items: items.slice(0, 90), LastEvaluatedKey: { N: 2 } } },
          { err: new Error("An AWS Error") },
        ]
      )
    );

    test(
      "Query Index gets data from an index",
      mockQuery(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
          const index = pipeline.createIndex("gsi1", { pk: "gsi1pk", sk: "gsi1sk" });
          let results = await index.query({ gsi1pk: "queryIndex:1", gsi1sk: sortKey("=", "100") }).all();

          // if the GSI isn't yet up to date, wait and run again
          if (results.length === 0) {
            const item = await pipeline.getItems([{ id: "query:1", sk: "99" }]).all();

            await new Promise((resolve) => setTimeout(resolve, 5000));
            results = await index.query({ gsi1pk: "queryIndex:1", gsi1sk: sortKey("=", "100") }).all();
          }

          expect(results.length).toEqual(1);
          expect(results[0]?.gsi1sk).toEqual("100");
        },
        [{ data: { Items: items.slice(-1) } }]
      )
    );
  });

  describe("Iterator", () => {
    const items = new Array(300).fill(0).map((_, i) => ({
      id: "iterator:1",
      sk: i.toString(),
      plusOne: i + 1,
      evenIsOne: i % 2 === 0 ? 1 : 0,
      other: new Array(120)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });
        await pipeline.putItems(items);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "Map Lazy gets all items when iterated",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          const results = await pipeline
            .query<{ plusOne: string }>({ id: "iterator:1" })
            .mapLazy((item) => item.plusOne)
            .all();

          expect(results.length).toEqual(300);
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 100), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(100, 150), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(150, 200), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(200, 250), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(250, 300) } },
        ]
      )
    );

    test(
      "Map Lazy waits until other method is called to execute iterator",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client })
            .withReadBatchSize(50)
            .withReadBuffer(1);

          let lazyCounter = 0;

          await pipeline
            .query<{ plusOne: string }>({ id: "iterator:1" })
            .mapLazy((item, index) => {
              lazyCounter = index;
              return item.plusOne;
            })
            .forEach(async (_item, index) => {
              await new Promise((resolve, reject) => setTimeout(resolve, 1));
              expect(lazyCounter - index).toBeLessThanOrEqual(50);
            });
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 100), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(100, 150), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(150, 200), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(200, 250), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(250, 300) } },
        ]
      )
    );

    test(
      "Filter Lazy waits until other method is called to execute iterator",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client })
            .withReadBatchSize(50)
            .withReadBuffer(1);

          let lazyCounter = 0;

          await pipeline
            .query<{ plusOne: string; evenIsOne: number }>({ id: "iterator:1" })
            .filterLazy((item, index) => {
              return item.evenIsOne % 2 === 1;
            })
            .mapLazy((item, index) => {
              lazyCounter = index;
              return { plusOneNum: parseInt(item.plusOne, 10), evenIsOne: item.evenIsOne };
            })
            .forEach(async (item, index) => {
              await new Promise((resolve, reject) => setTimeout(resolve, 1));
              expect(lazyCounter - index).toBeLessThanOrEqual(50);
              expect(item.plusOneNum % 2).toEqual(1);
            });
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 100), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(100, 150), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(150, 200), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(200, 250), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(250, 300) } },
        ]
      )
    );

    test(
      "Items returns an async generator for each item",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client });

          const iterator = pipeline.query<{ plusOne: number }>({ id: "iterator:1" }, { limit: 145 }).iterator();

          let index = 0;

          for await (const item of iterator) {
            index += 1;
            expect("plusOne" in item).toBeTruthy();
          }

          expect(index).toEqual(145);
        },
        [
          { data: { Items: items.slice(0, 50), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(50, 100), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(100, 150), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(150, 200), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(200, 250), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(250, 300) } },
        ]
      )
    );

    test(
      "ForEachStride loops through items in groups of batchSize",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBatchSize(50);

          await pipeline
            .query<{ plusOne: string }>({ id: "iterator:1" })
            .forEachStride((stride, strideIndex, _pipeline, cancel) => {
              expect(strideIndex).toBeLessThanOrEqual(4);
              expect(stride.length).toBeLessThanOrEqual(50);
              if (strideIndex === 4) {
                cancel();
              }
            });
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 300) } },
        ]
      )
    );

    test(
      "StrideIterator loops through items in groups of batchSize",
      mockQuery(
        async (client) => {
          const pipeline = new Pipeline(TEST_TABLE, { pk: "id", sk: "sk" }, { client }).withReadBatchSize(50);

          const iterator = pipeline
            .query<{ plusOne: string }>({ id: "iterator:1" })
            .strideIterator();

          let i = 0;

          for await (const group of iterator) {
            expect(group.length).toEqual(50);
            i += 1;
            if (i > 0) {
              break;
            }
          }
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200) } },
        ]
      )
    );
  });
});
