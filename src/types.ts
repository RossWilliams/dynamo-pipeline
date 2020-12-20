import { ExpressionAttributeNameMap } from "aws-sdk/clients/dynamodb";

export type Operator = "=" | "<" | ">" | "<=" | ">=" | "<>";
export type Logical = "AND" | "OR" | "NOT";
export type QueryOperator = Operator | "begins_with" | "between";
export type ConditionOperator = Operator | "contains" | "attribute_type";

export type ConditionFunction = "attribute_exists" | "attribute_not_exists";

export type KeyConditions = {
  pk: string | number | Buffer | Uint8Array;
  sk?: string;
  sk2?: string;
  skOperator?: QueryOperator;
};

export type DynamoCondition = {
  Condition: string;
  ExpressionAttributeNames?: ExpressionAttributeNameMap;
  ExpressionAttributeValues?: { [key: string]: any };
};

export type KeySet = {
  pk: string;
  sk?: string;
};

export type Index = KeySet & {
  name: string;
};

export type KeyType = string | number | Buffer | Uint8Array;
export type KeyTypeName = "N" | "S" | "B";
export type Key = { [x: string]: KeyType };
export type PrimitiveType = string | number | null | boolean | Buffer | Uint8Array;

export type PrimitiveTypeName = KeyTypeName | "NULL" | "BOOL";
export type PropertyTypeName = PrimitiveTypeName | "M" | "L";
export type DynamoValue = { [_key in PropertyTypeName]?: string | boolean };
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

type BaseExpression<T = Operator> = { lhs: LHSOperand; rhs: Operand; operator: T } | Between | BeginsWith;

export type ConditionExpression =
  | BaseExpression<ConditionOperator>
  | In
  | { property: string; operator: ConditionFunction }
  | {
      lhs?: ConditionExpression;
      rhs: ConditionExpression;
      logical: Logical;
    };

export type QueryExpression =
  | BaseExpression
  | {
      lhs?: QueryExpression;
      rhs: QueryExpression;
      logical: Logical;
    };

// removes the string option from type definition
export type UpdateReturnValues = "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW";
