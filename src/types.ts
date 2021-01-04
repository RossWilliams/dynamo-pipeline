export type Scalar = string | number; // binary is base64 encoded before being send
export type ComparisonOperator = "=" | "<" | ">" | "<=" | ">=" | "<>";
export type Logical = "AND" | "OR" | "NOT";
export type QueryOperator = "begins_with" | "between";
export type ConditionOperator = ComparisonOperator | "contains" | "attribute_type";
export type ConditionFunction = "attribute_exists" | "attribute_not_exists";

export type SKQuery =
  | `${Exclude<ComparisonOperator, "<>">} ${Scalar}`
  | `begins_with ${Scalar}`
  | `between ${Scalar} and ${Scalar}`;
export type SKQueryParts =
  | [Exclude<ComparisonOperator, "<>"> | "begins_with", Scalar]
  | ["between", Scalar, "and", Scalar];

export type KeyConditions = {
  pk: Scalar;
  sk?: SKQuery;
};

export type DynamoConditionAttributeName = `#p${number}`;
export type DynamoConditionAttributeValue = `:v${number}`;

export type DynamoCondition = {
  Condition: string;
  ExpressionAttributeNames?: Record<DynamoConditionAttributeName, string>;
  ExpressionAttributeValues?: Record<DynamoConditionAttributeValue, Scalar>;
};

export type KeyDefinition = {
  pk: string;
  sk?: string;
};

export type IndexDefinition = KeyDefinition & {
  name: string;
};

export type KeyType = string | number | Buffer | Uint8Array;
export type KeyTypeName = "N" | "S" | "B";
export type Key<KS extends KeyDefinition = { pk: "id" }> = Record<KS["pk"] | Exclude<KS["sk"], undefined>, Scalar>;
export type PrimitiveType = string | number | null | boolean | Buffer | Uint8Array;

export type PrimitiveTypeName = KeyTypeName | "NULL" | "BOOL";
export type PropertyTypeName = PrimitiveTypeName | "M" | "L";

export type DynamoValue = {
  [_key in PropertyTypeName]?: string | boolean | Array<DynamoValue> | Record<string, DynamoValue>;
};

export type DynamoPrimitiveValue = {
  [_key in PrimitiveTypeName]?: string | boolean | number;
};

export type Operand = { property: string } | { value: PrimitiveType } | { property: string; function: "size" };
export type LHSOperand = string | { property: string; function: "size" };

type BeginsWith = {
  property: string;
  operator: "begins_with";
  value: PrimitiveType;
};

type Between = {
  property: string;
  start: PrimitiveType;
  end: PrimitiveType;
  operator: "between";
};

type In = { property: string; list: PrimitiveType[]; operator: "in" };

type BaseExpression<T = ComparisonOperator> = { lhs: LHSOperand; rhs: Operand; operator: T } | Between | BeginsWith;

export type ConditionExpression =
  | BaseExpression<ConditionOperator>
  | In
  | { operator: ConditionFunction; property: string }
  | {
      lhs?: ConditionExpression;
      logical: Logical;
      rhs: ConditionExpression;
    };

// removes the string option from type definition
export type UpdateReturnValues = "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW";
