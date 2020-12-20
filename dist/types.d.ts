/// <reference types="node" />
import { ExpressionAttributeNameMap } from "aws-sdk/clients/dynamodb";
export declare type Operator = "=" | "<" | ">" | "<=" | ">=" | "<>";
export declare type Logical = "AND" | "OR" | "NOT";
export declare type QueryOperator = Operator | "begins_with" | "between";
export declare type ConditionOperator = Operator | "contains" | "attribute_type";
export declare type ConditionFunction = "attribute_exists" | "attribute_not_exists";
export declare type KeyConditions = {
    pk: string | number | Buffer | Uint8Array;
    sk?: string;
    sk2?: string;
    skOperator?: QueryOperator;
};
export declare type DynamoCondition = {
    Condition: string;
    ExpressionAttributeNames?: ExpressionAttributeNameMap;
    ExpressionAttributeValues?: {
        [key: string]: any;
    };
};
export declare type KeySet = {
    pk: string;
    sk?: string;
};
export declare type Index = KeySet & {
    name: string;
};
export declare type KeyType = string | number | Buffer | Uint8Array;
export declare type KeyTypeName = "N" | "S" | "B";
export declare type Key = {
    [x: string]: KeyType;
};
export declare type PrimitiveType = string | number | null | boolean | Buffer | Uint8Array;
export declare type PrimitiveTypeName = KeyTypeName | "NULL" | "BOOL";
export declare type PropertyTypeName = PrimitiveTypeName | "M" | "L";
export declare type DynamoValue = {
    [_key in PropertyTypeName]?: string | boolean;
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
declare type BaseExpression<T = Operator> = {
    lhs: LHSOperand;
    rhs: Operand;
    operator: T;
} | Between | BeginsWith;
export declare type ConditionExpression = BaseExpression<ConditionOperator> | In | {
    property: string;
    operator: ConditionFunction;
} | {
    lhs?: ConditionExpression;
    rhs: ConditionExpression;
    logical: Logical;
};
export declare type QueryExpression = BaseExpression | {
    lhs?: QueryExpression;
    rhs: QueryExpression;
    logical: Logical;
};
export declare type UpdateReturnValues = "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW";
export {};
