/// <reference types="node" />
export declare type Scalar = string | number;
export declare type ComparisonOperator = "=" | "<" | ">" | "<=" | ">=" | "<>";
export declare type Logical = "AND" | "OR" | "NOT";
export declare type QueryOperator = "begins_with" | "between";
export declare type ConditionOperator = ComparisonOperator | "contains" | "attribute_type";
export declare type ConditionFunction = "attribute_exists" | "attribute_not_exists";
export declare type SKQuery = `${Exclude<ComparisonOperator, "<>">} ${Scalar}` | `begins_with ${Scalar}` | `between ${Scalar} and ${Scalar}`;
export declare type SKQueryParts = [Exclude<ComparisonOperator, "<>"> | "begins_with", Scalar] | ["between", Scalar, "and", Scalar];
export declare type QueryTemplate = [Exclude<ComparisonOperator, "<>"> | "begins_with", Scalar] | ["between", "and", Scalar, Scalar];
export declare type DynamoConditionAttributeName = `#p${number}`;
export declare type DynamoConditionAttributeValue = `:v${number}`;
export declare type DynamoCondition = {
    Condition: string;
    ExpressionAttributeNames?: Record<DynamoConditionAttributeName, string>;
    ExpressionAttributeValues?: Record<DynamoConditionAttributeValue, Scalar>;
};
export declare type SimpleKey = {
    pk: string;
};
export declare type CompoundKey = {
    pk: string;
    sk: string;
};
export declare type KeyDefinition = SimpleKey | CompoundKey;
export declare type IndexDefinition = (SimpleKey & {
    name: string;
}) | (CompoundKey & {
    name: string;
});
export declare type KeyType = string | number | Buffer | Uint8Array;
export declare type KeyTypeName = "N" | "S" | "B";
export declare type Key<Keyset extends KeyDefinition> = Keyset extends CompoundKey ? Record<Keyset["pk"] | Keyset["sk"], Scalar> : Record<Keyset["pk"], Scalar>;
export declare type KeyConditions<Keyset extends KeyDefinition> = Keyset extends CompoundKey ? Record<Keyset["pk"], Scalar> & Partial<Record<Keyset["sk"], QueryTemplate>> : Record<Keyset["pk"], Scalar>;
export declare type PrimitiveType = string | number | null | boolean | Buffer | Uint8Array;
export declare type PrimitiveTypeName = KeyTypeName | "NULL" | "BOOL";
export declare type PropertyTypeName = PrimitiveTypeName | "M" | "L";
export declare type DynamoValue = {
    [_key in PropertyTypeName]?: string | boolean | Array<DynamoValue> | Record<string, DynamoValue>;
};
export declare type DynamoPrimitiveValue = {
    [_key in PrimitiveTypeName]?: string | boolean | number;
};
export declare type Operand = {
    property: string;
} | {
    value: PrimitiveType;
} | {
    property: string;
    function: "size";
};
export declare type LHSOperand = string | {
    property: string;
    function: "size";
};
declare type BeginsWith = {
    property: string;
    operator: "begins_with";
    value: PrimitiveType;
};
declare type Between = {
    property: string;
    start: PrimitiveType;
    end: PrimitiveType;
    operator: "between";
};
declare type In = {
    property: string;
    list: PrimitiveType[];
    operator: "in";
};
declare type BaseExpression<T = ComparisonOperator> = {
    lhs: LHSOperand;
    rhs: Operand;
    operator: T;
} | Between | BeginsWith;
export declare type ConditionExpression = BaseExpression<ConditionOperator> | In | {
    operator: ConditionFunction;
    property: string;
} | {
    lhs?: ConditionExpression;
    logical: Logical;
    rhs: ConditionExpression;
};
export declare type UpdateReturnValues = "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW";
export {};
