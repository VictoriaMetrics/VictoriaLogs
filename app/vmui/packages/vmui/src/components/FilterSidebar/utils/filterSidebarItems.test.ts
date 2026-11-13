import { describe, expect, it } from "vitest";
import { LogsFieldValues } from "../../../api/types";
import { filterSidebarItemsBySearch } from "./filterSidebarItems";

const values: LogsFieldValues[] = [
  { value: "frontend", hits: 10 },
  { value: "shipping", hits: 5 },
];

describe("filterSidebarItemsBySearch", () => {
  it("filters items by a trimmed, case-insensitive substring", () => {
    const result = filterSidebarItemsBySearch(values, " SHIP ");

    expect(result).toEqual([{ value: "shipping", hits: 5 }]);
  });

  it("returns an empty array when nothing matches", () => {
    expect(filterSidebarItemsBySearch(values, "service")).toEqual([]);
  });

  it("returns every value for an empty search", () => {
    expect(filterSidebarItemsBySearch(values, "  ")).toEqual(values);
  });

  it("preserves the input order and does not mutate items", () => {
    const items = [
      { value: "service.name", hits: 5 },
      { value: "host.name", hits: 10 },
      { value: "service.namespace", hits: 20 },
    ];
    const original = structuredClone(items);

    expect(filterSidebarItemsBySearch(items, "service")).toEqual([items[0], items[2]]);
    expect(items).toEqual(original);
  });
});
