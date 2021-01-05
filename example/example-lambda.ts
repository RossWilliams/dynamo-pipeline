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

export async function handler(event: AddUserCalendarEvent): Promise<{ error: string } | { item: CalendarItem }> {
  const examplePipeline = new Pipeline(TABLE_NAME, { pk: "pk", sk: "sk" })
    .withIndex("gsi1", { pk: "gsi1pk", sk: "gsi1sk" })
    .withReadBuffer(20);

  const startDate: string = event.startDateTime.split("T")[0] || "";
  const sevenDaysFromStart: string | null = tryGetSevenDaysFromStart(startDate);

  if (!startDate || !sevenDaysFromStart) {
    console.error("Error: Data format error. Invalid startDateTime", event.startDateTime);
    return { error: "Invalid startDateTime in Event" };
  }

  // update user profile
  const updatedUserVersion = await examplePipeline
    .update<{ currentVersion: number }>(
      { pk: event.userId, sk: event.userId }, // keys for the items to update
      { lastEventAdded: event.startDateTime, currentVersion: event.expectedVersion + 1 }, // attributes to update
      {
        // options object
        condition: {
          //type-safe and nestable conditions
          lhs: "currentVersion",
          operator: "=",
          rhs: { value: event.expectedVersion },
        },
        returnType: "UPDATED_NEW",
      }
    )
    .then((updatedValues) => updatedValues?.currentVersion);

  if (updatedUserVersion !== event.expectedVersion + 1) {
    console.info(`
    Info: Calendar Event not added. Version does not match expected version
      User Version: ${event.expectedVersion}
      Actual Version: ${updatedUserVersion}
    `);
    return { error: "Version Conflict Error" };
  }

  // delete all other calenar items
  await examplePipeline
    // sk string is type checked
    .queryIndex<CalendarItem>("gsi1", { pk: event.userId, sk: `between ${startDate} and ${sevenDaysFromStart}` })
    .forEach((event, pipeline) => pipeline.delete(event)); // delete method can take extra object properties and extract keys

  // add in new calendar item
  const newItem: CalendarItem = {
    pk: "1",
    sk: "1",
    gsi1pk: event.userId,
    gsi1sk: event.startDateTime,
    start: event.startDateTime,
  };

  await examplePipeline.putIfNotExists(newItem);

  return { item: newItem };
}

function tryGetSevenDaysFromStart(start: string): string | null {
  try {
    return new Date(new Date(start).setDate(new Date(start).getDate() + 7)).toISOString().split("T")[0] || null;
  } catch {
    return null;
  }
}
