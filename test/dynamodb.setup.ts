import DynamoDB from "aws-sdk/clients/dynamodb";

export async function ensureDatabaseExists(tableName: string): Promise<void> {
  const dynamodb = new DynamoDB();
  const tables = await dynamodb.listTables().promise();
  if (!tables || !tables.TableNames) {
    throw new Error("Could not list account tables\n\n" + tables.$response);
  }
  if (!tables.TableNames?.includes(tableName)) {
    await dynamodb
      .createTable({
        AttributeDefinitions: [
          { AttributeName: "id", AttributeType: "S" },
          { AttributeName: "sk", AttributeType: "S" },
          { AttributeName: "gsi1pk", AttributeType: "S" },
          { AttributeName: "gsi1sk", AttributeType: "S" },
        ],
        TableName: tableName,
        KeySchema: [
          { AttributeName: "id", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: "gsi1",
            KeySchema: [
              { AttributeName: "gsi1pk", KeyType: "HASH" },
              { AttributeName: "gsi1sk", KeyType: "RANGE" },
            ],
            Projection: {
              ProjectionType: "ALL",
            },
          },
        ],
        BillingMode: "PAY_PER_REQUEST",
      })
      .promise();

    let status = "CREATING";
    while (status === "CREATING") {
      await new Promise((resolve) => setTimeout(resolve, 5000));

      status = await dynamodb
        .describeTable({ TableName: tableName })
        .promise()
        .then((result) => {
          const indexStatus = result.Table?.GlobalSecondaryIndexes?.pop()?.IndexStatus;
          if (indexStatus !== "ACTIVE") {
            return "CREATING";
          }

          const tableStatus = result.Table?.TableStatus || "FAILURE";
          return tableStatus;
        });
    }
  }
}
