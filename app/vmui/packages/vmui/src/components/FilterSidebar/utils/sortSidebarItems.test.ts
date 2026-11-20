import { describe, expect, it } from "vitest";
import { LogsFieldValues } from "../../../api/types";
import { sortSidebarItems } from "./sortSidebarItems";
import { SortOptions } from "../types";

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

  it.each<{ sort: SortOptions; expected: string[] }>([
    { sort: { by: "name", direction: "asc" }, expected: ["a", "c", "b", "d"] },
    { sort: { by: "name", direction: "desc" }, expected: ["c", "a", "d", "b"] },
    { sort: { by: "hits", direction: "asc" }, expected: ["c", "a", "d", "b"] },
    { sort: { by: "hits", direction: "desc" }, expected: ["a", "c", "b", "d"] },
  ])("sorts within both groups for $sort.by $sort.direction without mutating input", ({ sort, expected }) => {
    const groupedItems = [
      { value: "d", hits: 20 },
      { value: "c", hits: 10 },
      { value: "b", hits: 40 },
      { value: "a", hits: 30 },
    ];
    const original = structuredClone(groupedItems);

    const result = sortSidebarItems(groupedItems, sort, new Set(["a", "c"]));

    expect(getValues(result)).toEqual(expected);
    expect(groupedItems).toEqual(original);
  });
});
