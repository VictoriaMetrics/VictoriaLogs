import { describe, it, expect } from "vitest";

import {
  getStreamPairs,
  getStreamKeys,
  getAllStreamKeys,
  convertToFieldFilter,
  calculateTotalHits,
  isEqualLogByKeys,
  removeLogsByKeys,
  parseGroupByFields,
  serializeGroupByFields,
  getFieldPairs,
  formatFieldPair,
} from "./logs";

import { LOGS_GROUP_BY, WITHOUT_GROUPING } from "../constants/logs";
import { TIME_FIELD } from "../constants/logFields";
import { LogHits, Logs } from "../api/types";

describe("utils/logs", () => {
  describe("getStreamPairs", () => {
    it("empty stream {}", () => {
      expect(getStreamPairs("{}")).toEqual([]);
    });

    it("single pair in braces", () => {
      expect(getStreamPairs("{a=\"1\"}")).toEqual(["a=\"1\""]);
    });

    it("multiple pairs in braces", () => {
      expect(getStreamPairs("{a=\"1\",b=\"2\",c=\"3\"}")).toEqual(["a=\"1\"", "b=\"2\"", "c=\"3\""]);
    });

    it("values may contain commas (must not split inside quotes)", () => {
      expect(getStreamPairs("{a=\"v,1\",b=\"2\"}")).toEqual(["a=\"v,1\"", "b=\"2\""]);
    });

    it("values may contain escaped quotes (must not break splitting)", () => {
      expect(getStreamPairs("{a=\"va\\\"l\",b=\"2\"}")).toEqual(["a=\"va\\\"l\"", "b=\"2\""]);
    });

    it("handles special characters and brackets inside quoted values", () => {
      expect(getStreamPairs("{a=\"v()[]{}<>!@#$%^&*_-+=:;?/.,\"}")).toEqual(["a=\"v()[]{}<>!@#$%^&*_-+=:;?/.,\""]);
    });
  });

  describe("getStreamKeys", () => {
    it("empty stream {}", () => {
      expect(getStreamKeys("{}")).toEqual([]);
    });

    it("single pair", () => {
      expect(getStreamKeys("{a=\"1\"}")).toEqual(["a"]);
    });

    it("multiple pairs", () => {
      expect(getStreamKeys("{a=\"1\",b=\"2\",c=\"3\"}")).toEqual(["a", "b", "c"]);
    });

    it("values may contain commas (must not break splitting)", () => {
      expect(getStreamKeys("{a=\"v,1\",b=\"2\"}")).toEqual(["a", "b"]);
    });

    it("values may contain escaped quotes (must not break splitting)", () => {
      expect(getStreamKeys("{a=\"va\\\"l\",b=\"2\"}")).toEqual(["a", "b"]);
    });
  });

  describe("getAllStreamKeys", () => {
    it("empty input -> []", () => {
      expect(getAllStreamKeys([])).toEqual([]);
    });

    it("collects unique keys from _stream across rows", () => {
      const data: Array<Record<string, unknown>> = [
        { _stream: "{a=\"1\",b=\"2\"}" },
        { _stream: "{b=\"3\",c=\"4\"}" },
      ];

      expect(getAllStreamKeys(data).sort()).toEqual(["a", "b", "c"]);
    });

    it("ignores rows without string _stream", () => {
      const data: Array<Record<string, unknown>> = [
        { _stream: "{a=\"1\"}" },
        { _stream: 123 },
        {},
        { _stream: null },
      ];

      expect(getAllStreamKeys(data).sort()).toEqual(["a"]);
    });
  });

  describe("parseGroupByFields", () => {
    it("no grouping -> []", () => {
      expect(parseGroupByFields(null)).toEqual([]);
      expect(parseGroupByFields("")).toEqual([]);
      expect(parseGroupByFields(WITHOUT_GROUPING)).toEqual([]);
    });

    it("single field", () => {
      expect(parseGroupByFields("level")).toEqual(["level"]);
    });

    it("multiple fields with empty items", () => {
      expect(parseGroupByFields("level,host,,_stream")).toEqual(["level", "host", "_stream"]);
    });

    it("drops the without grouping value", () => {
      expect(parseGroupByFields(`level,${WITHOUT_GROUPING}`)).toEqual(["level"]);
    });
  });

  describe("serializeGroupByFields", () => {
    it("empty fields -> without grouping", () => {
      expect(serializeGroupByFields([])).toBe(WITHOUT_GROUPING);
    });

    it("joins fields with comma", () => {
      expect(serializeGroupByFields(["level", "host"])).toBe("level,host");
    });
  });

  describe("getFieldPairs", () => {
    it("empty fields -> []", () => {
      expect(getFieldPairs({})).toEqual([]);
    });

    it("keeps regular fields as is", () => {
      expect(getFieldPairs({ level: "error", host: "vm-1" })).toEqual([
        { field: "level", value: "error" },
        { field: "host", value: "vm-1" },
      ]);
    });

    it("skips fields with empty values", () => {
      expect(getFieldPairs({ level: "", host: "vm-1" })).toEqual([{ field: "host", value: "vm-1" }]);
    });

    it("splits _stream into stream labels", () => {
      expect(getFieldPairs({ _stream: "{app=\"vmui\",host=\"vm-1\"}" })).toEqual([
        { field: "app", value: "vmui", isStream: true },
        { field: "host", value: "vm-1", isStream: true },
      ]);
    });

    it("unescapes quotes in stream label values", () => {
      expect(getFieldPairs({ _stream: "{app=\"va\\\"l\"}" })).toEqual([
        { field: "app", value: "va\"l", isStream: true },
      ]);
    });

    it("mixes stream labels with regular fields", () => {
      expect(getFieldPairs({ _stream: "{app=\"vmui\"}", level: "error" })).toEqual([
        { field: "app", value: "vmui", isStream: true },
        { field: "level", value: "error" },
      ]);
    });
  });

  describe("formatFieldPair", () => {
    it("formats a pair as field=\"value\"", () => {
      expect(formatFieldPair({ field: "level", value: "error" })).toBe("level=\"error\"");
    });

    it("escapes quotes in values", () => {
      expect(formatFieldPair({ field: "msg", value: "he said \"yo\"" })).toBe("msg=\"he said \\\"yo\\\"\"");
    });
  });

  describe("convertToFieldFilter", () => {
    it("converts key=\"value\" into key: \"value\"", () => {
      expect(convertToFieldFilter("foo=\"bar\"")).toBe("foo: \"bar\"");
    });

    it("uses default field (LOGS_GROUP_BY) for plain values", () => {
      expect(convertToFieldFilter("hello")).toBe(`${LOGS_GROUP_BY}: "hello"`);
    });

    it("uses provided field for plain values", () => {
      expect(convertToFieldFilter("hello", "service")).toBe("service: \"hello\"");
    });

    it("escapes quotes in plain values via JSON.stringify", () => {
      expect(convertToFieldFilter("he said \"yo\"", "msg")).toBe("msg: \"he said \\\"yo\\\"\"");
    });

    it("does not treat unquoted key=value as key/value (per current regex)", () => {
      expect(convertToFieldFilter("foo=bar", "x")).toBe("x: \"foo=bar\"");
    });
  });

  describe("calculateTotalHits", () => {
    it("returns 0 for empty array", () => {
      expect(calculateTotalHits([])).toBe(0);
    });

    it("sums totals and treats missing/0 as 0", () => {
      const hits = [{ total: 2 }, { total: 0 }, {}, { total: 5 }] as LogHits[];
      expect(calculateTotalHits(hits)).toBe(7);
    });
  });

  describe("isEqualLogByKeys", () => {
    it("returns true when _time and all fields are strictly equal", () => {
      const a = { _time: 1, foo: "bar", n: 2 } as unknown as Logs;
      const b = { _time: 1, foo: "bar", n: 2 } as unknown as Logs;
      expect(isEqualLogByKeys(a, b, [TIME_FIELD, "foo", "n"])).toBe(true);
    });

    it("returns false when _time differs", () => {
      const a = { _time: 1, foo: "bar" } as unknown as Logs;
      const b = { _time: 2, foo: "bar" } as unknown as Logs;
      expect(isEqualLogByKeys(a, b, [TIME_FIELD, "foo"])).toBe(false);
    });

    it("returns false when any field differs", () => {
      const a = { _time: 1, foo: "bar" } as unknown as Logs;
      const b = { _time: 1, foo: "baz" } as unknown as Logs;
      expect(isEqualLogByKeys(a, b, [TIME_FIELD, "foo"])).toBe(false);
    });

    it("treats missing vs present field as different", () => {
      const a = { _time: 1, foo: "bar" } as unknown as Logs;
      const b = { _time: 1, foo: "bar", x: 1 } as unknown as Logs;
      expect(isEqualLogByKeys(a, b, [TIME_FIELD, "foo", "x"])).toBe(false);
    });
  });

  describe("removeExactLog", () => {
    it("removes logs matching target exactly", () => {
      const target = { _time: 1, foo: "bar", x: 1 } as unknown as Logs;

      const logs = [
        target,
        { _time: 1, foo: "bar", x: 2 } as unknown as Logs,
        { _time: 2, foo: "bar", x: 1 } as unknown as Logs,
        { _time: 1, foo: "bar", x: 1 } as unknown as Logs, // same content
      ];

      expect(removeLogsByKeys(logs, target, [TIME_FIELD, "foo", "x"])).toEqual([
        { _time: 1, foo: "bar", x: 2 },
        { _time: 2, foo: "bar", x: 1 },
      ]);
    });

    it("returns same list if nothing matches", () => {
      const target = { _time: 9, foo: "x" } as unknown as Logs;
      const logs = [{ _time: 1, foo: "bar" } as unknown as Logs];

      expect(removeLogsByKeys(logs, target, [TIME_FIELD, "foo"])).toEqual([{ _time: 1, foo: "bar" }]);
    });
  });
});
