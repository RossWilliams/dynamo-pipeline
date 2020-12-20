import { Pipeline } from "../src/Pipeline";
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
} from "./helpers";
import { ensureDatabaseExists } from "./dynamodb.setup";

/*
When running against DynamoDB, ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION or AWS_DEFAULT_REGION set.
Alternativelym ensure an ~/.aws/credentials file is set.
If assuming a role, ensure ~/.aws/config file is set, and AWS_PROFILE is set, and AWS_SDK_LOAD_CONFIG=1 is set.
*/
const TEST_WITH_DYNAMO = process.env.TEST_WITH_DYNAMO === "true" || process.env.TEST_WITH_DYNAMO === "1";
const TEST_TABLE = process.env.TEST_WITH_DYNAMO_TABLE || "dynamo-pipeline-e677beee-e15b-44c4-bee2-e0558699b597";

describe("Dynamo Pipeline", () => {
  beforeAll(async () => {
    setMockOn(!TEST_WITH_DYNAMO);
    if (TEST_WITH_DYNAMO) {
      await ensureDatabaseExists(TEST_TABLE);
    }
    if (TEST_WITH_DYNAMO) {
      const pipeline = new Pipeline(TEST_TABLE);
      await pipeline
        .withKeys("id", "sk")
        .scan<{ id: string; sk: string }>()
        .forEach((item, _) => pipeline.delete(item.id, item.sk));
    }
  }, 30000);

  afterAll(async () => {
    if (TEST_WITH_DYNAMO) {
      const pipeline = new Pipeline(TEST_TABLE);
      await pipeline
        .withKeys("id", "sk")
        .scan<{ id: string; sk: string }>()
        .forEach((item, _) => pipeline.delete(item.id, item.sk));
    }
  });

  test("creates a pipeline", () => {
    const pipeline = new Pipeline(TEST_TABLE);
    expect(pipeline).toBeDefined();
  });

  test("updates pipeline config", () => {
    const pipeline = new Pipeline(TEST_TABLE);
    pipeline.withIndex("gsi1", "gsi1pk", "gsi1sk").withKeys("id", "sk").withReadBuffer(10).withWriteBuffer(20);
    expect(pipeline.config.indexes).toStrictEqual({ gsi1: { pk: "gsi1pk", sk: "gsi1sk", name: "gsi1" } });
    expect(pipeline.config.readBuffer).toEqual(10);
    expect(pipeline.config.writeBuffer).toEqual(20);
  });

  describe("Put Item", () => {
    test(
      "put item returns the same pipeline",
      mockPut(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });
        return expect(pipeline.put({ id: "put:1", sk: "1" })).resolves.toEqual(pipeline);
      })
    );

    test(
      "failure to put adds item to unprocessed array",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });
          await pipeline.putIfNotExists({ id: "put:1", sk: "1" });
          expect(pipeline.unprocessedItems.length).toEqual(1);
          const unprocessedItems = [];
          pipeline.handleUnprocessed((i) => unprocessedItems.push(i));
          expect(unprocessedItems.length).toEqual(1);
        },
        { err: new Error("item exists") }
      )
    );

    test(
      "put sends a formatted put to the document client",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });
        await pipeline.put({ id: "put:2", sk: "2" });
        const [[input]] = spy.calls;

        expect(input?.Item).toStrictEqual({ id: "put:2", sk: "2" });
        expect(input?.ConditionExpression).not.toBeDefined();
        expect(input?.TableName).toEqual(TEST_TABLE);
      })
    );

    test(
      "puts with attribute not exists condition",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
        });
        await pipeline.put({ id: "put:1", sk: "1" }, { operator: "attribute_not_exists", property: "x" });
        const [[input]] = spy.calls;
        expect(input?.Item).toStrictEqual({ id: "put:1", sk: "1" });
        expect(input?.ConditionExpression).toEqual("attribute_not_exists(#p0)");
        expect(input?.ExpressionAttributeNames).toStrictEqual({ "#p0": "x" });
        expect(input?.TableName).toEqual(TEST_TABLE);
      })
    );

    test(
      "puts with intersect of conditions",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });
        await pipeline.put(
          { id: "put:3", sk: "3" },
          {
            lhs: { operator: "attribute_not_exists", property: "id" },
            logical: "AND",
            rhs: {
              logical: "NOT",
              rhs: { operator: "attribute_type", lhs: "other", rhs: { value: "S" } },
            },
          }
        );
        expect(pipeline.unprocessedItems.length).toEqual(0);
        const [[input]] = spy.calls;
        expect(input?.ConditionExpression).toEqual("attribute_not_exists(#p0) AND (NOT attribute_type(#p1, :v0))");
        expect(input?.ExpressionAttributeNames).toStrictEqual({
          "#p0": "id",
          "#p1": "other",
        });
        expect(input?.ExpressionAttributeValues).toStrictEqual({
          ":v0": "S",
        });
      })
    );

    test(
      "putIfNotExists adds  pk condition",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client });

        pipeline.withKeys("id", "sk");
        await pipeline.putIfNotExists({ id: "put:4", sk: "4" });
        const [[input]] = spy.calls;
        expect(Object.keys(input.ExpressionAttributeNames || {}).length).toEqual(1);
        expect(Object.values(input.ExpressionAttributeNames || {})).toStrictEqual(["id"]);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "put with conditions can check if just an sk exists",
      mockPut(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });

          pipeline.withKeys("id", "sk");
          await pipeline.put({ id: "put:4", sk: "4" }, { operator: "attribute_not_exists", property: "sk" });
          const [[input]] = spy.calls;
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
      "put with conditions can check value operators as valid",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client });

        pipeline.withKeys("id", "sk");
        await pipeline.put({ id: "put:4", sk: "4" }, { lhs: "sk", operator: "<", rhs: { value: "5" } });

        const [[input]] = spy.calls;
        expect(input.ExpressionAttributeValues).toStrictEqual({ ":v0": "5" });
        expect(input.ExpressionAttributeNames).toStrictEqual({ "#p0": "sk" });
        expect(input.ConditionExpression).toEqual("#p0 < :v0");
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "put with conditions can check value operators as invalid",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });

          pipeline.withKeys("id", "sk");
          await pipeline.put({ id: "put:4", sk: "4" }, { lhs: "sk", operator: ">", rhs: { value: "5" } });

          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "put with conditions can check BETWEEN operator as valid",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client });
        pipeline.withKeys("id", "sk");

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

        // eslint-disable-next-line
        const input = spy.calls[2][0];

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
      "put with conditions can check BETWEEN operator as invalid",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          let unprocessedCount = 0;
          pipeline.withKeys("id", "sk");

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
      "put with conditions can check IN operator as valid",
      mockPut(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client });
        pipeline.withKeys("id", "sk");

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
        const input = spy.calls[1][0];

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
      "put with conditions can check IN as invalid",
      mockPut(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          let unprocessedCount = 0;
          pipeline.withKeys("id", "sk");

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
      "putItems with more than 25 items batches into multiple writes",
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");

        const items = new Array(53).fill(0).map((_, i) => ({
          id: "putMany:" + i,
          sk: i.toString(),
          other: new Array(150)
            .fill(0)
            .map(() => Math.random().toString(36).substring(2, 15))
            .join(""),
        }));

        await pipeline.putItems(items);
        expect(pipeline.unprocessedItems.length).toEqual(0);
      })
    );

    test(
      "putItems with invalid item returns the invalid chunk.",
      mockBatchWrite(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");

          const items: any[] = new Array(53).fill(0).map((_, i) => ({
            id: "putMany:" + i,
            sk: i.toString(),
            other: new Array(150)
              .fill(0)
              .map(() => Math.random().toString(36).substring(2, 15))
              .join(""),
          }));
          items.push({ id: "putMany:bad", other: 1 });
          items.push({ id: "putMany:bad2", other: 1 });
          await pipeline.putItems(items);
          expect(pipeline.unprocessedItems.length).toEqual(5);
        },
        [{ data: {} }, { data: {} }, { err: new Error("err") }]
      )
    );

    test(
      "putItems with processing error returns unprocessed item.",
      alwaysMockBatchWrite(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");

          const items = [
            { id: "putMany:good", sk: "2", other: 1 },
            { id: "putMany:bad2", sk: "2", other: 1 },
          ];
          await pipeline.putItems(items);
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        {
          data: {
            UnprocessedItems: {
              [TEST_TABLE]: [{ PutRequest: { Item: { id: "putMany:bad2", sk: "2", other: 1 } } }],
            },
          },
        }
      )
    );
  });

  describe("Update Item", () => {
    beforeAll(
      mockPut(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });

        await pipeline.put({ id: "update:1", sk: "1", other: 1 });
      })
    );

    test(
      "Update item returns new or old values",
      mockUpdate(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });

          const result = await pipeline.update("update:1", "1", { other: 2 }, undefined, "UPDATED_NEW");
          expect(result).toStrictEqual({ other: 2 });
          const result2 = await pipeline.update("update:1", "1", { other: 3 }, undefined, "UPDATED_OLD");
          expect(result2).toStrictEqual({ other: 2 });
        },
        { data: { Attributes: { other: 2 } } }
      )
    );

    test(
      "Update item with valid condition updates item",
      mockUpdate(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });
          const result = await pipeline.update(
            "update:1",
            "1",
            { other: 4 },
            { lhs: "other", operator: ">", rhs: { value: 0 } },
            "UPDATED_NEW"
          );
          const [[input]] = spy.calls;
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
      "Update item with invalid condition does not update item",
      mockUpdate(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });
          const result = await pipeline.update(
            "update:1",
            "1",
            { other: 5 },
            { lhs: "other", operator: "<", rhs: { value: 0 } },
            "UPDATED_NEW"
          );
          const [[input]] = spy.calls;
          expect(result).toEqual(null);
          expect(input.ConditionExpression).toEqual("#p1 < :v1");
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "Update item without required sort key fails",
      mockUpdate(async (client, spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });

        expect(() =>
          pipeline.update(
            "update:1" as string,
            undefined,
            { other: 5 },
            { lhs: "other", operator: "<", rhs: { value: 0 } },
            "UPDATED_NEW"
          )
        ).toThrow();
      })
    );
  });

  describe("Delete Item", () => {
    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });

        await pipeline.putItems([
          { id: "delete:1", sk: "1", other: 1 },
          { id: "delete:2", sk: "2", other: 2 },
          { id: "delete:3", sk: "3", other: 3 },
        ]);
      })
    );

    test(
      "Delete item returns old values",
      mockDelete(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });

          const result = await pipeline.delete("delete:1", "1", undefined, "ALL_OLD");
          expect(result).toStrictEqual({ other: 1, id: "delete:1", sk: "1" });
        },
        { data: { Attributes: { other: 1, id: "delete:1", sk: "1" } } }
      )
    );

    test(
      "Delete item with valid condition deletes item",
      mockDelete(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });
          const result = await pipeline.delete(
            "delete:2",
            "2",
            { lhs: "other", operator: ">", rhs: { value: 0 } },
            "ALL_OLD"
          );
          const [[input]] = spy.calls;
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
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });
          const result = await pipeline.delete(
            "delete:3",
            "3",
            { lhs: "other", operator: "<", rhs: { value: 0 } },
            "ALL_OLD"
          );
          const [[input]] = spy.calls;
          expect(result).toEqual(null);
          expect(input.ConditionExpression).toEqual("#p0 < :v0");
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );

    test(
      "Delete item without required sort key fails",
      mockDelete(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, {
          client,
          tableKeys: { pk: "id", sk: "sk" },
        });

        expect(() => pipeline.delete("delete:1")).toThrow();
      })
    );

    // note by default we set a condition to error if the item does not exist.
    test(
      "Delete item not in db adds to unprocessed list",
      mockDelete(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, {
            client,
            tableKeys: { pk: "id", sk: "sk" },
          });
          const result = await pipeline.delete("delete:3", "4");
          expect(result).toEqual(null);
          expect(pipeline.unprocessedItems.length).toEqual(1);
        },
        { err: new Error("err") }
      )
    );
  });

  describe("Scan", () => {
    const items = new Array(250).fill(0).map((_, i) => ({
      id: "scan:" + i,
      sk: i.toString(),
      plusOne: (i + 1).toString(),
      other: new Array(600)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");

        await pipeline.putItems(items);
      })
    );

    test(
      "Scan will fetch multiple times to get all items",
      mockScan(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(100, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });

          const result = await scanner.all();
          expect(result.length).toEqual(250);
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 250) } },
        ]
      )
    );

    test(
      "Scan filtering can have properties on both sides of operator",
      mockScan(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          type Data = { id: string; sk: string; other: string };

          const scanner = pipeline.withKeys("id", "sk").scan<Data>(100, undefined, undefined, {
            lhs: { property: "id", operator: "begins_with", value: "scan:" },
            logical: "AND",
            rhs: { lhs: "sk", operator: "<", rhs: { property: "plusOne" } },
          });

          const result = await scanner.all();
          // 100 < 99, 10 < 9 when working in strings, 248 means all OK!
          expect(result.length).toEqual(248);
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 248) } },
        ]
      )
    );

    test(
      "Scan will limit the amount of items returned",
      mockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(100, 200, undefined, { property: "id", operator: "begins_with", value: "scan:" });

          const result: Data[] = await scanner.all();
          expect(result.length).toEqual(200);
          expect(spy.calls.length).toEqual(2);
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 250) } },
        ]
      )
    );

    test(
      "Scan will limit the amount of items returned",
      mockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          type Data = { id: string; sk: string; other: string };

          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(100, 200, undefined, { property: "id", operator: "begins_with", value: "scan:" });

          const result: Data[] = await scanner.all();
          expect(result.length).toEqual(200);
          expect(spy.calls.length).toEqual(2);
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 250) } },
        ]
      )
    );

    test(
      "Scan will fetch a second time before the user starts to process content",
      mockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(100, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });

          let processing = false;
          await scanner.forEach(() => {
            if (!processing) {
              expect(spy.calls.length).toEqual(2);
            }
            processing = true;
          });
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 250) } },
        ],
        1
      )
    );

    test(
      "Scan will batch results to allow pausing to fetch more",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client, readBuffer: 1 });
          type Data = { id: string; sk: string; other: string };
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(10, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });

          let checked = false;
          let index = 0;
          await scanner.forEach((_item) => {
            if (!checked && index === 90) {
              expect(spy.calls.length).toEqual(2);
              checked = true;
            }
            index += 1;
          });

          expect(spy.calls.length).toEqual(4);
        },
        [
          { data: { Items: items.slice(0, 100), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(100, 200), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(200, 220), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(200, 250) } },
        ]
      )
    );

    test(
      "Scan will not exceed the configuration buffer limit",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client, readBuffer: 1 });
          type Data = { id: string; sk: string; other: string };
          const result: Data[] = [];
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(5, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });
          await scanner.forEach((i) => {
            result.push(i);
            // a single response is 20 items, so a read buffer of 1 will still buffer 20 items.
            expect((spy.calls.length - 1) * 20 - result.length).toBeLessThanOrEqual(20);

            return new Promise<void>((resolve) => setImmediate(() => resolve(), 5));
          });

          expect(result.length).toEqual(220);
        },
        [
          { data: { Items: items.slice(0, 20), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(20, 40), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(40, 60), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(60, 80), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(80, 100), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(100, 120), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(120, 140), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(140, 160), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(160, 180), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(180, 200), LastEvaluatedKey: { N: 10 } } },
          { data: { Items: items.slice(200, 220) } },
        ],
        1
      )
    );

    test(
      "Scan with filter returning empty results array does not cause fetcher to stall.",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client, readBuffer: 1 });
          type Data = { id: string; sk: string; other: string };

          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(5, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });
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
          { data: { Items: items.slice(180, 200), LastEvaluatedKey: { N: 10 } } },
          { data: { Items: items.slice(200, 220) } },
        ],
        1
      )
    );

    test(
      "Scan will not buffer additional items if read buffer set to zero",
      alwaysMockScan(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client, readBuffer: 0 });
          type Data = { id: string; sk: string; other: string };

          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(5, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });

          const result: Data[] = await scanner.map((i, index) => {
            // a single response is 20 items, so a read buffer of 0 will still buffer 20 items.
            expect(spy.calls.length * 20 - index).toBeLessThanOrEqual(20 + 5);

            return i;
          });

          expect(result.length).toEqual(220);
        },
        [
          { data: { Items: items.slice(0, 20), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(20, 40), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(40, 60), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(60, 80), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(80, 100), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(100, 120), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(120, 140), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(140, 160), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(160, 180), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(180, 200), LastEvaluatedKey: { N: 10 } } },
          { data: { Items: items.slice(200, 220) } },
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
          const pipeline = new Pipeline(TEST_TABLE, { client, readBuffer });
          type Data = { id: string; sk: string; other: string };
          const result: Data[] = [];
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(batchSize, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });
          await scanner.forEach((i) => {
            result.push(i);
            // find the buffered items, subtract out processed items, should be less than the max buffer.
            expect((spy.calls.length - 1) * 20 - result.length).toBeLessThanOrEqual(batchSize * readBuffer + batchSize);

            return new Promise<void>((resolve) => setImmediate(() => resolve(), 5));
          });

          expect(result.length).toEqual(220);
        },
        [
          { data: { Items: items.slice(0, 20), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(20, 40), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(40, 60), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(60, 80), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(80, 100), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(100, 120), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(120, 140), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(140, 160), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(160, 180), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(180, 200), LastEvaluatedKey: { N: 10 } } },
          { data: { Items: items.slice(200, 220) } },
        ],
        1
      )
    );

    test(
      "Scan processing in a forEach which returns false stops processing and fetching",
      alwaysMockScan(
        async (client, spy) => {
          const readBuffer = 5;
          const batchSize = 10;
          const pipeline = new Pipeline(TEST_TABLE, { client, readBuffer });
          type Data = { id: string; sk: string; other: string };
          const result: Data[] = [];
          const scanner = pipeline
            .withKeys("id", "sk")
            .scan<Data>(batchSize, undefined, undefined, { property: "id", operator: "begins_with", value: "scan:" });
          await scanner.forEach((i) => {
            result.push(i);
            if (result.length === 8) {
              return false;
            }
            // find the buffered items, subtract out processed items, should be less than the max buffer.
            return new Promise<void>((resolve) => setImmediate(() => resolve(), 5));
          });

          expect(spy.calls.length).toEqual(2);
          expect(result.length).toEqual(8);
        },
        [
          { data: { Items: items.slice(0, 20), LastEvaluatedKey: { N: 1 } } },
          { data: { Items: items.slice(20, 40), LastEvaluatedKey: { N: 2 } } },
          { data: { Items: items.slice(40, 60), LastEvaluatedKey: { N: 3 } } },
          { data: { Items: items.slice(60, 80), LastEvaluatedKey: { N: 4 } } },
          { data: { Items: items.slice(80, 100), LastEvaluatedKey: { N: 5 } } },
          { data: { Items: items.slice(100, 120), LastEvaluatedKey: { N: 6 } } },
          { data: { Items: items.slice(120, 140), LastEvaluatedKey: { N: 7 } } },
          { data: { Items: items.slice(140, 160), LastEvaluatedKey: { N: 8 } } },
          { data: { Items: items.slice(160, 180), LastEvaluatedKey: { N: 9 } } },
          { data: { Items: items.slice(180, 200), LastEvaluatedKey: { N: 10 } } },
          { data: { Items: items.slice(200, 220) } },
        ],
        1
      )
    );
  });

  describe("Get Items", () => {
    const items = new Array(250).fill(0).map((_, i) => ({
      id: "getItems:" + i,
      sk: i.toString(),
      other: new Array(6)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");

        await pipeline.putItems(items);
      })
    );

    test(
      "Get 2 items returns both items",
      mockBatchGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");
          const result = await pipeline
            .getItems<{ other: string }>([
              { id: "getItems:1", sk: "1" },
              { id: "getItems:2", sk: "2" },
            ])
            .all();

          expect(result.length).toEqual(2);
          expect(result[0].other).toBeDefined();
        },
        { data: { Responses: { [TEST_TABLE]: items.slice(0, 2) } } }
      )
    );

    test(
      "Get 200 items returns all items",
      mockBatchGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk").withReadBuffer(4);
          const results = await pipeline.getItems<{ other: string }>(items.slice(0, 200)).all();

          expect(results.length).toEqual(200);
        },
        [
          { data: { Responses: { [TEST_TABLE]: items.slice(0, 100) } } },
          { data: { Responses: { [TEST_TABLE]: items.slice(100, 200) } } },
        ]
      )
    );

    test(
      "Get with unprocessed keys retries the request across two requests",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk").withReadBuffer(4);

          const all = await pipeline.getItems<{ other: string }>(items.slice(0, 100), 20).all();

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
      "Get with partial unprocessed keys retries the request across two requests",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk").withReadBuffer(2);

          const all = await pipeline.getItems<{ other: string }>(items.slice(0, 100), 20).all();

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
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk").withReadBuffer(2);

          const all = await pipeline.getItems<{ other: string }>(items.slice(0, 100), 20).all();

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
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk").withReadBuffer(2);
          let index = 0;
          await pipeline.getItems<{ other: string }>(items.slice(0, 100), 20).forEach((_item) => {
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
          { data: { Responses: { [TEST_TABLE]: items.slice(100, 120) } } },
        ],
        10
      )
    );

    test(
      "Get can return many empty results without stalling pipeline",
      alwaysMockBatchGet(
        async (client, spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk").withReadBuffer(2);
          const result = await pipeline.getItems<{ other: string }>(items.slice(0, 120), 20).all();
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
  });

  describe("Transact Get", () => {
    const items = new Array(250).fill(0).map((_, i) => ({
      id: "transactGet:" + i,
      sk: i.toString(),
      other: new Array(6)
        .fill(0)
        .map(() => Math.random().toString(36).substring(2, 15))
        .join(""),
    }));

    beforeAll(
      mockBatchWrite(async (client, _spy) => {
        const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");

        await pipeline.putItems(items);
      })
    );

    test(
      "Get 2 items returns both items",
      mockTransactGet(
        async (client, _spy) => {
          const pipeline = new Pipeline(TEST_TABLE, { client }).withKeys("id", "sk");
          const result = await pipeline
            .transactGet<{ other: string }>([
              { id: "transactGet:1", sk: "1" },
              { id: "transactGet:2", sk: "2" },
            ])
            .all();

          expect(result.length).toEqual(2);
          expect(result[0].other).toBeDefined();
        },
        { data: { Responses: { [TEST_TABLE]: items.slice(0, 2) } } }
      )
    );
  });

  xdescribe("Query", () => {});

  xdescribe("Query Index", () => {});
});
