import { ConditionExpression, DynamoCondition, KeyDefinition, QueryTemplate } from "../types";
export declare function conditionToDynamo(condition: ConditionExpression | undefined, mergeCondition?: DynamoCondition): DynamoCondition;
export declare const pkName: (keys: KeyDefinition) => string;
export declare function skQueryToDynamoString(template: QueryTemplate): string;
