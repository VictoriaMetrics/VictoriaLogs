import { describe, it, expect, vi, Mock, afterEach } from "vitest";
import { getFromStorage, saveToStorage } from "../../utils/storage";
import {
  addQueryToHistoryStorage,
  clearQueryHistoryStorage,
  formatHistoryDate,
  getHistoryFromStorage,
  groupHistoryByDay,
  removeQueryFromHistoryStorage
} from "./utils";
import { MAX_QUERIES_HISTORY, MAX_QUERY_FIELDS } from "../../constants/logs";

vi.mock("../../utils/storage", () => ({
  getFromStorage: vi.fn(),
  saveToStorage: vi.fn(),
}));

describe("utils", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.resetAllMocks();
  });

  describe("getHistoryFromStorage", () => {
    it("should return QUERY_HISTORY entries from storage", () => {
      const getFromStorageMock = getFromStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
        "QUERY_HISTORY_META": { "first_query": 1000 },
      }));

      expect(getHistoryFromStorage()).toStrictEqual([
        { query: "first_query", lastRunAt: 1000 },
      ]);
    });

    it("should return an empty array if QUERY_HISTORY is missing", () => {
      const getFromStorageMock = getFromStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_FAVORITES": [["favorite_query"]],
      }));

      expect(getHistoryFromStorage()).toStrictEqual([]);
    });

    it("should return entries without lastRunAt if QUERY_HISTORY_META is missing", () => {
      const getFromStorageMock = getFromStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
      }));

      expect(getHistoryFromStorage()).toStrictEqual([
        { query: "first_query", lastRunAt: undefined },
      ]);
    });
  });

  describe("addQueryToHistoryStorage", () => {
    it("should not save empty query", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
      }));

      addQueryToHistoryStorage(" ");
      expect(saveToStorageMock).not.toHaveBeenCalled();
    });

    it("should not change QUERY_HISTORY cause add the same query", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      vi.spyOn(Date, "now").mockReturnValue(1000);
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
      }));

      addQueryToHistoryStorage("first_query");
      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[[\"first_query\"]],\"QUERY_HISTORY_META\":{\"first_query\":1000}}"
      );
    });

    it("should add new query to the first position to QUERY_HISTORY", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      vi.spyOn(Date, "now").mockReturnValue(2000);
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
        "QUERY_HISTORY_META": { "first_query": 1000 },
      }));

      addQueryToHistoryStorage("new_query");
      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[[\"new_query\",\"first_query\"]],\"QUERY_HISTORY_META\":{\"new_query\":2000,\"first_query\":1000}}"
      );
    });

    it("should move existing query to the first position in QUERY_HISTORY", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      vi.spyOn(Date, "now").mockReturnValue(4000);
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query", "second_query", "third_query"]],
        "QUERY_HISTORY_META": {
          "first_query": 1000,
          "second_query": 2000,
          "third_query": 3000,
        },
      }));

      addQueryToHistoryStorage("third_query");
      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[[\"third_query\",\"first_query\",\"second_query\"]],\"QUERY_HISTORY_META\":{\"third_query\":4000,\"first_query\":1000,\"second_query\":2000}}"
      );
    });

    it("should limit the QUERY_HISTORY if add extra query", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      vi.spyOn(Date, "now").mockReturnValue(1000);
      const maxQueries = MAX_QUERIES_HISTORY * MAX_QUERY_FIELDS;
      const currentHistory = (new Array(maxQueries)).fill(1).map((_, i) => `${i}_query`);
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [currentHistory],
      }));

      addQueryToHistoryStorage("extra_query");

      const calls = saveToStorageMock.mock.calls;
      const firstCallArgs = calls[0];
      expect(firstCallArgs[0]).toStrictEqual("LOGS_QUERY_HISTORY");
      const savedQueries = JSON.parse(firstCallArgs[1]);
      expect(savedQueries["QUERY_HISTORY"][0][0]).toStrictEqual("extra_query");
      expect(savedQueries["QUERY_HISTORY"][0].length).toStrictEqual(maxQueries);
      expect(savedQueries["QUERY_HISTORY_META"]["extra_query"]).toStrictEqual(1000);
    });

    it("should preserve other storage entries", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      vi.spyOn(Date, "now").mockReturnValue(2000);
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
        "QUERY_HISTORY_META": { "first_query": 1000 },
        "QUERY_FAVORITES": [["favorite_query"]],
      }));

      addQueryToHistoryStorage("new_query");

      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[[\"new_query\",\"first_query\"]],\"QUERY_HISTORY_META\":{\"new_query\":2000,\"first_query\":1000},\"QUERY_FAVORITES\":[[\"favorite_query\"]]}"
      );
    });
  });

  describe("removeQueryFromHistoryStorage", () => {
    it("should remove query from QUERY_HISTORY and QUERY_HISTORY_META", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query", "second_query", "third_query"]],
        "QUERY_HISTORY_META": {
          "first_query": 1000,
          "second_query": 2000,
          "third_query": 3000,
        },
      }));

      removeQueryFromHistoryStorage("second_query");

      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[[\"first_query\",\"third_query\"]],\"QUERY_HISTORY_META\":{\"first_query\":1000,\"third_query\":3000}}"
      );
    });

    it("should preserve other storage entries", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query", "second_query"]],
        "QUERY_HISTORY_META": {
          "first_query": 1000,
          "second_query": 2000,
        },
        "QUERY_FAVORITES": [["favorite_query"]],
      }));

      removeQueryFromHistoryStorage("first_query");

      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[[\"second_query\"]],\"QUERY_HISTORY_META\":{\"second_query\":2000},\"QUERY_FAVORITES\":[[\"favorite_query\"]]}"
      );
    });
  });

  describe("clearQueryHistoryStorage", () => {
    it("should clear QUERY_HISTORY and preserve other storage entries", () => {
      const getFromStorageMock = getFromStorage as Mock;
      const saveToStorageMock = saveToStorage as Mock;
      getFromStorageMock.mockReturnValue(JSON.stringify({
        "QUERY_HISTORY": [["first_query"]],
        "QUERY_HISTORY_META": { "first_query": 1000 },
        "QUERY_FAVORITES": [["favorite_query"]],
      }));

      clearQueryHistoryStorage();

      expect(saveToStorageMock).toHaveBeenCalledWith(
        "LOGS_QUERY_HISTORY",
        "{\"QUERY_HISTORY\":[],\"QUERY_HISTORY_META\":{},\"QUERY_FAVORITES\":[[\"favorite_query\"]]}"
      );
    });
  });

  describe("formatHistoryDate", () => {
    it("should return empty string for empty timestamp", () => {
      expect(formatHistoryDate()).toStrictEqual("");
    });

    it("should format timestamp as hours and minutes", () => {
      const timestamp = new Date(2026, 5, 19, 9, 5).getTime();

      expect(formatHistoryDate(timestamp)).toStrictEqual("09:05");
    });
  });

  describe("groupHistoryByDay", () => {
    it("should group entries by today, yesterday, formatted date and earlier", () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date(2026, 5, 19, 12, 0));

      const groups = groupHistoryByDay([
        { query: "today_query", lastRunAt: new Date(2026, 5, 19, 9, 5).getTime() },
        { query: "yesterday_query", lastRunAt: new Date(2026, 5, 18, 9, 5).getTime() },
        { query: "old_query", lastRunAt: new Date(2026, 5, 17, 9, 5).getTime() },
        { query: "unknown_time_query" },
      ]);

      expect(groups).toStrictEqual([
        {
          title: "Today - Friday, 19 June 2026",
          entries: [{ query: "today_query", lastRunAt: new Date(2026, 5, 19, 9, 5).getTime() }],
        },
        {
          title: "Yesterday - Thursday, 18 June 2026",
          entries: [{ query: "yesterday_query", lastRunAt: new Date(2026, 5, 18, 9, 5).getTime() }],
        },
        {
          title: "Wednesday, 17 June 2026",
          entries: [{ query: "old_query", lastRunAt: new Date(2026, 5, 17, 9, 5).getTime() }],
        },
        {
          title: "Earlier",
          entries: [{ query: "unknown_time_query" }],
        },
      ]);
    });

    it("should show newer queries above older queries within the same day", () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date(2026, 5, 19, 12, 0));

      const firstTimestamp = new Date(2026, 5, 19, 9, 5).getTime();
      const secondTimestamp = new Date(2026, 5, 19, 10, 5).getTime();

      expect(groupHistoryByDay([
        { query: "first_query", lastRunAt: firstTimestamp },
        { query: "second_query", lastRunAt: secondTimestamp },
      ])).toStrictEqual([
        {
          title: "Today - Friday, 19 June 2026",
          entries: [
            { query: "second_query", lastRunAt: secondTimestamp },
            { query: "first_query", lastRunAt: firstTimestamp },
          ],
        },
      ]);
    });
  });
});
