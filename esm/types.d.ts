/// <reference types="node" />
export type Scalar = string | number;
export type ComparisonOperator = "=" | "<" | ">" | "<=" | ">=" | "<>";
export type Logical = "AND" | "OR" | "NOT";
export type QueryOperator = "begins_with" | "between";
export type ConditionOperator = ComparisonOperator | "contains" | "attribute_type";
export type ConditionFunction = "attribute_exists" | "attribute_not_exists";
export type SKQuery = `${Exclude<ComparisonOperator, "<>">} ${Scalar}` | `begins_with ${Scalar}` | `between ${Scalar} and ${Scalar}`;
export type SKQueryParts = [Exclude<ComparisonOperator, "<>"> | "begins_with", Scalar] | ["between", Scalar, "and", Scalar];
export type QueryTemplate = [Exclude<ComparisonOperator, "<>"> | "begins_with", Scalar] | ["between", "and", Scalar, Scalar];
export type DynamoConditionAttributeName = `#p${number}`;
export type DynamoConditionAttributeValue = `:v${number}`;
export type DynamoCondition = {
    Condition: string;
    ExpressionAttributeNames?: Record<DynamoConditionAttributeName, string>;
    ExpressionAttributeValues?: Record<DynamoConditionAttributeValue, Scalar>;
};
export type SimpleKey = {
    pk: string;
};
export type CompoundKey = {
    pk: string;
    sk: string;
};
export type KeyDefinition = SimpleKey | CompoundKey;
export type IndexDefinition = (SimpleKey & {
    name: string;
}) | (CompoundKey & {
    name: string;
});
export type KeyType = string | number | Buffer | Uint8Array;
export type KeyTypeName = "N" | "S" | "B";
export type Key<Keyset extends KeyDefinition> = Keyset extends CompoundKey ? Record<Keyset["pk"] | Keyset["sk"], Scalar> : Record<Keyset["pk"], Scalar>;
export type KeyConditions<Keyset extends KeyDefinition> = Keyset extends CompoundKey ? Record<Keyset["pk"], Scalar> & Partial<Record<Keyset["sk"], QueryTemplate>> : Record<Keyset["pk"], Scalar>;
export type PrimitiveType = string | number | null | boolean | Buffer | Uint8Array;
export type PrimitiveTypeName = KeyTypeName | "NULL" | "BOOL";
export type PropertyTypeName = PrimitiveTypeName | "M" | "L";
export type DynamoValue = {
    [_key in PropertyTypeName]?: string | boolean | Array<DynamoValue> | Record<string, DynamoValue>;
};
export type DynamoPrimitiveValue = {
    [_key in PrimitiveTypeName]?: string | boolean | number;
};
export type Operand = {
    property: string;
} | {
    value: PrimitiveType;
} | {
    property: string;
    function: "size";
};
export type LHSOperand = string | {
    property: string;
    function: "size";
};
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
type In = {
    property: string;
    list: PrimitiveType[];
    operator: "in";
};
type BaseExpression<T = ComparisonOperator> = {
    lhs: LHSOperand;
    rhs: Operand;
    operator: T;
} | Between | BeginsWith;
export type ConditionExpression = BaseExpression<ConditionOperator> | In | {
    operator: ConditionFunction;
    property: string;
} | {
    lhs?: ConditionExpression;
    logical: Logical;
    rhs: ConditionExpression;
};
export type UpdateReturnValues = "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW";
export {};
