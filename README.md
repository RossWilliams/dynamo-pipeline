# dynamo-pipeline

Alternative API for DynamoDB DocumentClient to improve types, allow easy iteration and paging of data, and reduce developer mistakes. From "So complex there are no obvious mistakes" to "So simple there are obviously no mistakes".

## Status - API unstable, pin exact commit hash

Code is functional, but developer ergonomics and data structures will change significantly.

## Example

Suppose you wish to find the first 5000 form items with sk > "0000" which are not deleted, and for each item, add 'gsi1pk' and 'gsi1sk' attributes to each item.

### Dynamo Pipeline

```typescript
import { Pipeline } from "dynamo-pipeline";

interface Item {
  id: string;
  sk: string;
  _isDeleted: boolean;
  data: {
    attr1: string;
    attr2: string;
  };
}

const privatePipeline = new Pipeline("PrivateTableName-xxx", { pk: "id", sk: "sk" });

await privatePipeline
  .query<Item>(
    { pk: "FormId", sk: "> 0000" },
    {
      limit: 5000,
      filters: {
        lhs: { property: "_isDeleted" },
        logical: "<>",
        rhs: false,
      },
    }
  )
  .forEach((item, pipeline) => pipeline.update(item, { gsi1pk: data.attr1, gsi1sk: data.attr2 }));
```

### NoSQL Workbench Generated Code + Looping logic

```javascript
const AWS = require("aws-sdk");

const dynamoDbClient = createDynamoDbClient();
const queryInput = createQueryInput();
let isFirstQuery = true;
let itemsProcessed = 0;

while ((isFirstQuery || queryInput.LastEvaluatedKey) && itemsProcessed < 5000) {
  isFirstQuery = false;
  const result = await executeQuery(dynamoDbClient, queryInput);
  await Promise.all(
    result.Items.map((item) => {
      itemsProcessed += 1;
      if (itemsProcessed <= 5000) {
        return executeUpdateItem(
          dynamoDbClient,
          createUpdateItemInput(item.id, item.sk, item.data.attr1, item.data.attr2)
        );
      }
    })
  );

  if (result.LastEvaluatedKey) {
    queryInput.LastEvaluatedKey = result.LastEvaluatedKey;
  }
}

function createDynamoDbClient() {
  return new AWS.DynamoDB();
}

function createQueryInput() {
  return {
    TableName: "Private-xxx-xxx",
    ScanIndexForward: false,
    ConsistentRead: false,
    KeyConditionExpression: "#254c0 = :254c0 And #254c1 > :254c1",
    FilterExpression: "#254c2 <> :254c2",
    ExpressionAttributeValues: {
      ":254c0": {
        S: "FormId",
      },
      ":254c1": {
        S: "0000",
      },
      ":254c2": {
        BOOL: true,
      },
    },
    ExpressionAttributeNames: {
      "#254c0": "id",
      "#254c1": "sk",
      "#254c2": "_isDeleted",
    },
  };
}

function createUpdateItemInput(id, sk, gsi1pk, gsi1sk) {
  return {
    TableName: "Form-xxx-xxx",
    Key: {
      id: {
        S: id,
      },
      sk: {
        S: sk,
      },
    },
    UpdateExpression: "SET #4bd90 = :4bd90, #4bd91 = :4bd91",
    ExpressionAttributeValues: {
      ":4bd90": {
        S: gsi1pk,
      },
      ":4bd91": {
        S: gsi1sk,
      },
    },
    ExpressionAttributeNames: {
      "#4bd90": "gsi1pk",
      "#4bd91": "gsi1sk",
    },
  };
}

async function executeUpdateItem(dynamoDbClient, updateItemInput) {
  // Call DynamoDB's updateItem API
  try {
    const updateItemOutput = await dynamoDbClient.updateItem(updateItemInput).promise();
    return updateItemOutput;
  } catch (err) {
    //handleUpdateItemError(err);
  }
}

async function executeQuery(dynamoDbClient, queryInput) {
  try {
    const queryOutput = await dynamoDbClient.query(queryInput).promise();
    return queryOutput;
  } catch (err) {
    // handleQueryError(err);
  }
}
```

## Limitations

1. Partition Keys and Sort Keys only support string type
1. Multi-table get transactions are untested
1. Multi-table put transactions are untested
