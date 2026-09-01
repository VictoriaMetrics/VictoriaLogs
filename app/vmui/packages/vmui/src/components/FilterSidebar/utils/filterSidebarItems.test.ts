import { describe, expect, it } from "vitest";
import { LogsFieldValues } from "../../../api/types";
import {
  filterFilterSidebarValues,
  matchesFilterSidebarSearch,
} from "./filterSidebarItems";

const values: LogsFieldValues[] = [
  { value: "frontend", hits: 10 },
  { value: "shipping", hits: 5 },
];

describe("matchesFilterSidebarSearch", () => {
  it("matches a field name", () => {
    expect(matchesFilterSidebarSearch("service.name", [], "SERVICE")).toBe(true);
  });

  it("matches an available field value", () => {
    expect(matchesFilterSidebarSearch("service.name", values, "SHIP")).toBe(true);
  });

  it("does not match unavailable values", () => {
    expect(matchesFilterSidebarSearch("service.name", [], "shipping")).toBe(false);
  });

  it("matches an empty search", () => {
    expect(matchesFilterSidebarSearch("service.name", [], "  ")).toBe(true);
  });
});

describe("filterFilterSidebarValues", () => {
  it("filters values of an open field", () => {
    const result = filterFilterSidebarValues(values, "service.name", "SHIP");

    expect(result).toEqual([{ value: "shipping", hits: 5 }]);
  });

  it("returns every value when the field name matches", () => {
    expect(filterFilterSidebarValues(values, "service.name", "service")).toEqual(values);
  });

  it("returns every value for an empty search", () => {
    expect(filterFilterSidebarValues(values, "service.name", "")).toEqual(values);
  });
});
