import {
  ConditionExpression,
  DynamoCondition,
  DynamoConditionAttributeValue,
  KeyDefinition,
  LHSOperand,
  Operand,
  PrimitiveType,
  Scalar,
  QueryTemplate,
} from "../types";

/**
 * Takes a condition object structure and transforms it into a dynamodb filter expression
 * and sets the expression attribute names and values. Uses a simple counter for property
 * names and values, such that properties names are of the format #p{index} and property values
 * are of the format :v{index}.
 *
 * @param condition The condition object tree
 * @param mergeCondition Used for recursion only
 * @returns A DynamoCondition object which can be inserted into a dynamodb document client request.
 */
export function conditionToDynamo(
  condition: ConditionExpression | undefined,
  mergeCondition?: DynamoCondition
): DynamoCondition {
  const result: DynamoCondition =
    mergeCondition ||
    ({
      Condition: "",
    } as DynamoCondition);

  if (!condition) {
    return result;
  }

  // logical conditions are AND and OR conditions, where the left and right side each contain
  // their own nested condition expression. Each side is recursively called, and the result
  // added to the main condition object.
  if ("logical" in condition) {
    const preCondition = result.Condition;
    const logicalLhs = conditionToDynamo(condition.lhs, result);

    const logicalRhs = conditionToDynamo(condition.rhs, {
      Condition: preCondition,
      ExpressionAttributeNames: {
        ...result.ExpressionAttributeNames,
        ...logicalLhs.ExpressionAttributeNames,
      },
      ExpressionAttributeValues: {
        ...result.ExpressionAttributeValues,
        ...logicalLhs.ExpressionAttributeValues,
      },
    });
    // Note - take care in parenthesis, dynamodb will throw a bad request error if there are
    // superfluous parenthesis in
    if (condition.lhs && "logical" in condition.lhs) {
      logicalLhs.Condition = `(${logicalLhs.Condition})`;
    }
    if (condition.rhs && "logical" in condition.rhs) {
      logicalRhs.Condition = `(${logicalRhs.Condition})`;
    }
    result.Condition = `${logicalLhs.Condition + (logicalLhs.Condition.length ? " " : "")}${condition.logical} ${
      logicalRhs.Condition
    }`;

    // combine left and ride side name and value objects

    Object.entries({
      ...logicalRhs.ExpressionAttributeNames,
      ...logicalLhs.ExpressionAttributeNames,
    }).forEach(([name, value]) => {
      if (!result.ExpressionAttributeNames) {
        result.ExpressionAttributeNames = {};
      }
      // @ts-expect-error: Object.entries hard codes string as the key type,
      // and indexing by template strings is invalid in ts 4.2.0
      result.ExpressionAttributeNames[name] = value;
    });

    (Object.entries<Scalar>({
      ...logicalRhs.ExpressionAttributeValues,
      ...logicalLhs.ExpressionAttributeValues,
    }) as [DynamoConditionAttributeValue, Scalar][]).forEach(([name, value]) => {
      if (!result.ExpressionAttributeValues) {
        result.ExpressionAttributeValues = {};
      }

      // @ts-expect-error:  Object.entries hard codes string as the key type
      // and indexing by template strings is invalid in ts 4.2.0
      result.ExpressionAttributeValues[name] = value;
    });

    return result;
  }

  const names = conditionToAttributeNames(
    condition,
    result.ExpressionAttributeNames ? Object.keys(result.ExpressionAttributeNames).length : 0
  );

  const values = conditionToAttributeValues(
    condition,
    result.ExpressionAttributeValues ? Object.keys(result.ExpressionAttributeValues).length : 0
  );

  const conditionString = conditionToConditionString(
    condition,
    result.ExpressionAttributeNames ? Object.keys(result.ExpressionAttributeNames).length : 0,
    result.ExpressionAttributeValues ? Object.keys(result.ExpressionAttributeValues).length : 0,
    Object.keys(names).length
  );

  return {
    ...((Object.keys(names).length > 0 || Object.keys(result.ExpressionAttributeNames || {}).length > 0) && {
      ExpressionAttributeNames: { ...names, ...result.ExpressionAttributeNames },
    }),
    ...((Object.keys(values).length > 0 || Object.keys(result.ExpressionAttributeValues || {}).length > 0) && {
      ExpressionAttributeValues: { ...values, ...result.ExpressionAttributeValues },
    }),
    Condition: conditionString,
  };
}

/**
 * Converts a QueryTemplate structure to a string for sort keys in a query operation.
 */
export function skQueryToDynamoString(template: QueryTemplate): string {
  const expression: ConditionExpression =
    template[0] === "begins_with"
      ? { operator: template[0], property: "sk", value: template[1] }
      : template[0] === "between"
      ? { operator: template[0], property: "sk", start: template[2], end: template[3] }
      : { operator: template[0], lhs: "sk", rhs: { value: template[1] } };

  const result = conditionToConditionString(expression, 1, 1, 1);
  return result;
}

/**
 * Construct a dynamodb condition string for comparison operators.
 * Sets all names and values to #p1{index} or :v${index} where index starts
 * with nameStart or ValueStart and increments for each property or value.
 */
function comparisonOperator(
  condition: {
    lhs: LHSOperand;
    rhs: Operand;
    operator: ">" | "<" | ">=" | "<=" | "=" | "<>";
  },
  nameStart: number,
  valueStart: number
): string {
  // handle the case where a property could be accessed a map value, such as "mapProp.ItemProp"
  let lhs: string;
  if (typeof condition.lhs === "string") {
    lhs = condition.lhs
      .split(".")
      .map((_v, i) => {
        const name = "#p" + nameStart.toString();
        nameStart += 1;
        return name;
      })
      .join(".");
  } else {
    lhs = condition.lhs.property
      .split(".")
      .map((_v, i) => {
        const name = "#p" + nameStart.toString();
        nameStart += 1;
        return name;
      })
      .join(".");
  }

  // handle the case where a property could be accessed a map value, such as "mapProp.ItemProp"
  const rhs: string =
    "property" in condition.rhs
      ? condition.rhs.property
          .split(".")
          .map((_v, i) => {
            const name = "#p" + nameStart.toString();
            nameStart += 1;
            return name;
          })
          .join(".")
      : ":v" + valueStart.toString();

  const openingParens =
    typeof condition.lhs !== "string" && "function" in condition.lhs ? condition.lhs.function + "(" : "";
  const closingParens = "function" in condition.rhs ? ")" : "";

  return `${openingParens}${lhs}${typeof condition.lhs !== "string" && "function" in condition.lhs ? ")" : ""} ${
    condition.operator
  } ${"function" in condition.rhs ? condition.rhs.function + "(" : ""}${rhs}${closingParens}`;
}

function conditionToConditionString(
  condition: ConditionExpression,
  nameCountStart: number,
  valueCountStart: number,
  numberOfNameValues: number
): string {
  // TODO: HACK: the name and value conversions follow the same operator flow
  // as the condition to values and condition to names to keep the numbers in sync
  // lhs, rhs, start,end,list
  // lhs, rhs, property, arg2
  if ("logical" in condition) {
    /* istanbul ignore next */
    throw new Error("Unimplemented");
  }

  const nameStart = nameCountStart;
  let valueStart = valueCountStart;

  const singlePropName = new Array(numberOfNameValues)
    .fill(0)
    .map((_x, index) => `#p${index + nameStart}`)
    .join(".");

  switch (condition.operator) {
    case ">":
    case "<":
    case ">=":
    case "<=":
    case "=":
    case "<>":
      // TODO: fix any type
      return comparisonOperator(condition as any, nameStart, valueStart);
    case "begins_with":
    case "contains":
    case "attribute_type":
      return `${condition.operator}(${singlePropName}, :v${valueStart})`;
    case "attribute_exists":
    case "attribute_not_exists":
      return `${condition.operator}(${singlePropName})`;
    case "between":
      return `${singlePropName} BETWEEN :v${valueStart} AND :v${valueStart + 1}`;
    case "in":
      return `${singlePropName} IN (${condition.list
        .map(() => {
          valueStart += 1;
          return `:v${valueStart - 1}`;
        })
        .join(",")})`;
    default:
      /* istanbul ignore next */
      throw new Error("Operator does not exist");
  }
}

function conditionToAttributeValues(condition: ConditionExpression, countStart = 0): { [key: string]: any } {
  const values: { [key: string]: any } = {};

  if ("rhs" in condition && condition.rhs && "value" in condition.rhs) {
    setPropertyValue(condition.rhs.value, values, countStart);
  }

  if ("value" in condition) {
    setPropertyValue(condition.value, values, countStart);
  }

  if ("start" in condition) {
    setPropertyValue(condition.start, values, countStart);
  }

  if ("end" in condition) {
    setPropertyValue(condition.end, values, countStart);
  }

  if ("list" in condition) {
    condition.list.forEach((l) => setPropertyValue(l, values, countStart));
  }

  return values;
}

function setPropertyValue(value: PrimitiveType, values: { [key: string]: PrimitiveType }, countStart: number) {
  // note this is the main place to change if we switch from document client to the regular dynamodb client
  const dynamoValue = Array.isArray(value)
    ? value.join("")
    : typeof value === "boolean" || typeof value === "string" || typeof value === "number"
    ? value
    : value === null
    ? true
    : value?.toString() || true;

  return setRawPropertyValue(dynamoValue, values, countStart);
}

function setRawPropertyValue(value: PrimitiveType, values: { [key: string]: any }, countStart: number) {
  const name: string = ":v" + (Object.keys(values).length + countStart).toString();
  values[name] = value;
  return values;
}

function conditionToAttributeNames(condition: ConditionExpression, countStart = 0): { [key: string]: string } {
  const names: { [key: string]: string } = {};
  if ("lhs" in condition && condition.lhs && (typeof condition.lhs === "string" || "property" in condition.lhs)) {
    splitAndSetPropertyName(
      typeof condition.lhs === "string" ? condition.lhs : condition.lhs.property,
      names,
      countStart
    );
  }

  // TODO: Test if this is possible in a scan wih dynamo?
  if ("rhs" in condition && condition.rhs && "property" in condition.rhs) {
    splitAndSetPropertyName(condition.rhs.property, names, countStart);
  }

  if ("property" in condition) {
    splitAndSetPropertyName(condition.property, names, countStart);
  }

  return names;
}

function splitAndSetPropertyName(propertyName: string, names: { [key: string]: string }, countStart: number) {
  return propertyName
    .split(".")
    .forEach((prop) => (names["#p" + (Object.keys(names).length + countStart).toString()] = prop));
}
