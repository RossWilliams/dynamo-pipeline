import { ConditionExpression, DynamoCondition, QueryTemplate } from "../types";
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
export declare function conditionToDynamo(condition: ConditionExpression | undefined, mergeCondition?: DynamoCondition): DynamoCondition;
/**
 * Converts a QueryTemplate structure to a string for sort keys in a query operation.
 */
export declare function skQueryToDynamoString(template: QueryTemplate): string;
