import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Pipeline } from "../src/index";

const TABLE_NAME = process.env.TABLE_NAME || "example-2c19f773";

interface AddUserCalendarEvent {
  userId: string;
  startDateTime: string; //ISO 8601
  expectedVersion: number; // counting number
}

interface CalendarItem {
  pk: string;
  sk: string;
  gsi1pk: string;
  gsi1sk: string;
  start: string; //ISO 8601
}

function tryGetSevenDaysFromStart(start: string): string | null {
  try {
    return new Date(new Date(start).setDate(new Date(start).getDate() + 7)).toISOString().split("T")[0] || null;
  } catch {
    return null;
  }
}

export async function handler(event: AddUserCalendarEvent, client?: DocumentClient): Promise<void> {
  const examplePipeline = new Pipeline(TABLE_NAME, { pk: "pk", sk: "sk" })
    .withIndex("gsi1", { pk: "gsi1pk", sk: "gsi1sk" })
    .withReadBuffer(20);

  const startDate: string = event.startDateTime.split("T")[0] || "";
  const sevenDaysFromStart: string | null = tryGetSevenDaysFromStart(startDate);

  if (!startDate || !sevenDaysFromStart) {
    throw new Error("Invalid startDateTime in Event");
  }

  // update user profile
  const updatedUserVersion = await examplePipeline
    .update(
      { pk: event.userId, sk: event.userId },
      { lastEventAdded: event.startDateTime, currentVersion: event.expectedVersion + 1 },
      {
        condition: {
          lhs: "currentVersion",
          operator: "=",
          rhs: { value: event.expectedVersion },
        },
        returnType: "UPDATED_NEW",
      }
    )
    .then((updatedValues) => updatedValues?.currentVersion?.N);

  if (updatedUserVersion !== event.expectedVersion + 1) {
    return;
  }

  // delete all other calenar items
  await examplePipeline
    .queryIndex<CalendarItem>("gsi1", { pk: event.userId, sk: `between ${startDate} and ${sevenDaysFromStart}` })
    .forEach((event, pipeline) => pipeline.delete(event));

  // add in new calendar item
  const newItem: CalendarItem = {
    pk: "1",
    sk: "1",
    gsi1pk: event.userId,
    gsi1sk: event.startDateTime,
    start: event.startDateTime,
  };

  await examplePipeline.putIfNotExists(newItem);
}
