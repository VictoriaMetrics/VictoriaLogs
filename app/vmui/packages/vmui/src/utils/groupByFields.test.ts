import { beforeEach, describe, expect, it } from "vitest";
import {
  addRecentGroupByField,
  buildGroupByOptions,
  getRecentGroupByFields,
  getUniqueFieldNames,
} from "./groupByFields";
import { GROUP_BY_RECENT_LIMIT, WITHOUT_GROUPING } from "../constants/logs";

const STORAGE_KEY = "VLUI:LOGS_GROUP_BY_RECENT";

const tenant = { AccountID: "0", ProjectID: "0" };
const otherTenant = { AccountID: "1", ProjectID: "2" };

describe("recent group by fields", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it("keeps the most recent field first without duplicates", () => {
    addRecentGroupByField(tenant, "host");
    addRecentGroupByField(tenant, "level");
    addRecentGroupByField(tenant, "host");

    expect(getRecentGroupByFields(tenant)).toEqual(["host", "level"]);
  });

  it("keeps up to GROUP_BY_RECENT_LIMIT fields", () => {
    for (let i = 0; i <= GROUP_BY_RECENT_LIMIT; i++) {
      addRecentGroupByField(tenant, `field${i}`);
    }

    const fields = getRecentGroupByFields(tenant);
    expect(fields).toHaveLength(GROUP_BY_RECENT_LIMIT);
    expect(fields[0]).toBe(`field${GROUP_BY_RECENT_LIMIT}`);
    expect(fields).not.toContain("field0");
  });

  it("stores fields separately for each tenant", () => {
    addRecentGroupByField(tenant, "host");
    addRecentGroupByField(otherTenant, "service");

    expect(getRecentGroupByFields(tenant)).toEqual(["host"]);
    expect(getRecentGroupByFields(otherTenant)).toEqual(["service"]);
  });

  it("ignores \"none\" and noise fields", () => {
    addRecentGroupByField(tenant, WITHOUT_GROUPING);
    addRecentGroupByField(tenant, "_msg");
    addRecentGroupByField(tenant, "_time");
    addRecentGroupByField(tenant, "");

    expect(getRecentGroupByFields(tenant)).toEqual([]);
    expect(window.localStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it("ignores corrupted storage", () => {
    window.localStorage.setItem(STORAGE_KEY, "not a json");
    expect(getRecentGroupByFields(tenant)).toEqual([]);

    window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ value: { "0:0": "host" } }));
    expect(getRecentGroupByFields(tenant)).toEqual([]);

    window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ value: { "0:0": ["host", 42] } }));
    expect(getRecentGroupByFields(tenant)).toEqual(["host"]);
  });
});

describe("getUniqueFieldNames", () => {
  it("returns sorted unique keys of all logs", () => {
    const base = { _msg: "m", _stream: "{}", _time: "2026-01-01T00:00:00Z" };
    const logs = [
      { ...base, host: "h1" },
      { ...base, level: "info" },
      { ...base, host: "h2", app: "api" },
    ];

    expect(getUniqueFieldNames(logs)).toEqual(["_msg", "_stream", "_time", "app", "host", "level"]);
  });
});

describe("buildGroupByOptions", () => {
  it("orders options as none, current, recent, then the rest alphabetically", () => {
    const options = buildGroupByOptions({
      current: "service",
      recent: ["level", "host"],
      loaded: ["zone", "app"],
      fetched: ["region", "app"],
    });

    expect(options).toEqual([WITHOUT_GROUPING, "service", "level", "host", "app", "region", "zone"]);
  });

  it("removes duplicates across sources", () => {
    const options = buildGroupByOptions({
      current: "host",
      recent: ["host", "level"],
      loaded: ["host", "level", "app"],
      fetched: ["app", "level"],
    });

    expect(options).toEqual([WITHOUT_GROUPING, "host", "level", "app"]);
  });

  it("excludes noise fields from every source", () => {
    const options = buildGroupByOptions({
      current: "_msg",
      recent: ["_time"],
      loaded: ["_msg", "_time", "host"],
      fetched: ["_msg", "_time", "level"],
    });

    expect(options).toEqual([WITHOUT_GROUPING, "host", "level"]);
  });

  it("shows the current and recent fields even if no fields were loaded or fetched yet", () => {
    const options = buildGroupByOptions({ current: "host", recent: ["level"], loaded: [], fetched: [] });

    expect(options).toEqual([WITHOUT_GROUPING, "host", "level"]);
  });
});
