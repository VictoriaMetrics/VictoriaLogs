import { describe, expect, it } from "vitest";
import { LogsFieldValues } from "../../../api/types";
import { sortSidebarItems } from "./sortSidebarItems";

const items: LogsFieldValues[] = [
  { value: "host-10", hits: 10 },
  { value: "host-2", hits: 20 },
  { value: "database", hits: 20 },
];

const getValues = (values: LogsFieldValues[]) => values.map(({ value }) => value);

describe("sortSidebarItems", () => {
  it("sorts by hits in descending order", () => {
    const result = sortSidebarItems(items, { by: "hits", direction: "desc" });

    expect(getValues(result)).toEqual(["database", "host-2", "host-10"]);
  });

  it("sorts by hits in ascending order", () => {
    const result = sortSidebarItems(items, { by: "hits", direction: "asc" });

    expect(getValues(result)).toEqual(["host-10", "database", "host-2"]);
  });

  it("sorts by name in ascending order", () => {
    const result = sortSidebarItems(items, { by: "name", direction: "asc" });

    expect(getValues(result)).toEqual(["database", "host-2", "host-10"]);
  });

  it("sorts by name in descending order", () => {
    const result = sortSidebarItems(items, { by: "name", direction: "desc" });

    expect(getValues(result)).toEqual(["host-10", "host-2", "database"]);
  });

  it("keeps selected items before unselected items", () => {
    const result = sortSidebarItems(
      items,
      { by: "hits", direction: "desc" },
      new Set(["host-10"]),
    );

    expect(getValues(result)).toEqual(["host-10", "database", "host-2"]);
  });
});
