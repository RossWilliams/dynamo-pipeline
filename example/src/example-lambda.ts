// import { Pipeline, sortKey } from "dynamo-pipeline";
import { Pipeline, sortKey } from "../../";
const TABLE_NAME = process.env.TABLE_NAME || "example-2c19f773";
interface AddUserCalendarEvent {
  userId: string;
  startDateTime: string; // ISO 8601
  expectedVersion: number; // counting number
}

interface CalendarItem {
  id: string;
  sort: string;
  gsi1pk: string;
  gsi1sk: string;
  start: string; // ISO 8601
}

/**
 * 1. Update the user profile with appropriate event metadata
 * 2. Remove all existing events for the user for 7 days beyond the start date of the event
 * 3. Add the new calendar item
 * @param event The Calendar Event to add to the User
 */
export async function handler(event: AddUserCalendarEvent): Promise<{ error: string } | { item: CalendarItem }> {
  const table = new Pipeline(TABLE_NAME, { pk: "id", sk: "sort" }).withReadBuffer(20);
  const index = table.createIndex("gsi1", { pk: "gsi1pk", sk: "gsi1sk" });

  const startDate: string = event.startDateTime.split("T")[0] || "";
  const sevenDaysFromStart: string | null = tryGetSevenDaysFromStart(startDate);

  if (!startDate || !sevenDaysFromStart) {
    console.error("Error: Data format error. Invalid startDateTime", event.startDateTime);
    return { error: "Invalid startDateTime in Event" };
  }

  // update user profile
  const updatedUserVersion = await table
    .update<{ currentVersion: number }>(
      { id: event.userId, sort: event.userId }, // keys for the items to update
      { lastEventAdded: event.startDateTime, currentVersion: event.expectedVersion + 1 }, // attributes to update
      {
        // options object
        condition: {
          // type-safe and nestable conditions
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
      Actual Version: ${updatedUserVersion ?? ""}
    `);
    return { error: "Version Conflict Error" };
  }

  // delete all other calenar items
  await index
    // sk string is type checked
    .query<CalendarItem>({
      gsi1pk: event.userId,
      gsi1sk: sortKey("between", startDate, sevenDaysFromStart),
    })
    // delete method returns a promise which the forEach will await on for us.
    .forEach((event, _index) => table.delete(event)); // delete method can take extra object properties and extract keys

  // add in new calendar item
  const newItem: CalendarItem = {
    id: "1",
    sort: "1",
    gsi1pk: event.userId,
    gsi1sk: event.startDateTime,
    start: event.startDateTime,
  };

  // helper extention to put which add a pk not exists condition check to the request
  await table.putIfNotExists(newItem);

  return { item: newItem };
}

function tryGetSevenDaysFromStart(start: string): string | null {
  try {
    return new Date(new Date(start).setDate(new Date(start).getDate() + 7)).toISOString().split("T")[0] || null;
  } catch {
    return null;
  }
}
