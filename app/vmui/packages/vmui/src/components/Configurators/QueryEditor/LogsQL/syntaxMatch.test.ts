import { describe, expect, it } from "vitest";
import { getLogsQLGhostText } from "./syntaxMatch";

describe("hint", () => {
  describe("pipe name hints", () => {
    it("uses the syntax matcher once the pipe name is complete", () => {
      expect(getLogsQLGhostText("* | st")).toBe("ats by (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stat")).toBe("s by (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats")).toBe(" <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | extr")).toBe("act <pattern> from <field>");
      expect(getLogsQLGhostText("* | extract")).toBe(" <pattern> from <field>");
      expect(getLogsQLGhostText("* | extract_")).toBe("regexp <pattern> from <field>");
    });

    it("does not reuse the previous pipe after a trailing pipe separator", () => {
      expect(getLogsQLGhostText("|")).toBe("");
      expect(getLogsQLGhostText("| ")).toBe("");
      expect(getLogsQLGhostText("* |")).toBe("");
      expect(getLogsQLGhostText("* | ")).toBe("");
      expect(getLogsQLGhostText("* | stats count(\"a\", \"b\") |")).toBe("");
      expect(getLogsQLGhostText("* | stats count(\"a\", \"b\") | ")).toBe("");
      expect(getLogsQLGhostText("* | stats count(\"a\", \"b\") | st")).toBe("ats by (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count(\"a\", \"b\") | extract_regexp")).toBe(" <pattern> from <field>");
    });

    it("ignores pipe separators inside double quotes", () => {
      expect(getLogsQLGhostText("* | extract_regexp \"a|b\"")).toBe(" from <field>");
      expect(getLogsQLGhostText("* | extract_regexp \"a|b\" |")).toBe("");
      expect(getLogsQLGhostText("* | extract_regexp \"a|b\" | st")).toBe("ats by (<fields>) <func> if (<filter>) as <name>");
    });

    it("does not duplicate optional text after a slot", () => {
      expect(getLogsQLGhostText("* | extract_regexp abc from")).toBe(" <field>");
      expect(getLogsQLGhostText("* | extract_regexp abc from ")).toBe("<field>");
      expect(getLogsQLGhostText("* | extract abc from")).toBe(" <field>");
      expect(getLogsQLGhostText("* | format abc as")).toBe(" <name>");
    });
  });

  describe("stats-like function hints", () => {
    it("shows generic syntax ghost text for count pipe", () => {
      expect(getLogsQLGhostText("* | count")).toBe("(<fields>) as <name>");
      expect(getLogsQLGhostText("* | count(")).toBe("<fields>) as <name>");
      expect(getLogsQLGhostText("* | count()")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | count(*)")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | count(host)")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | count() as")).toBe(" <name>");
      expect(getLogsQLGhostText("* | count() as ")).toBe("<name>");
    });

    it("shows function ghost text for stats", () => {
      expect(getLogsQLGhostText("* | stats c")).toBe("ount([<fields>]) if (<filter>) as <name>");
    });

    it("does not duplicate the separator after stats space", () => {
      expect(getLogsQLGhostText("* | stats ")).toBe("<func> if (<filter>) as <name>");
    });

    it("skips optional parent clauses when matching a stats function", () => {
      expect(getLogsQLGhostText("* | stats co")).toBe("unt([<fields>]) if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count")).toBe("([<fields>]) if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count(")).toBe("<fields>) if (<filter>) as <name>");
    });

    it("prefers parent by clause before stats function", () => {
      expect(getLogsQLGhostText("* | stats b")).toBe("y (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats by")).toBe(" (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats by (")).toBe("<fields>) <func> if (<filter>) as <name>");
    });

    it("shows function ghost text for stats after by clause", () => {
      expect(getLogsQLGhostText("* | stats by (host) c")).toBe("ount([<fields>]) if (<filter>) as <name>");
    });

    it("does not duplicate the separator after stats by clause", () => {
      expect(getLogsQLGhostText("* | stats by (host) ")).toBe("<func> if (<filter>) as <name>");
    });

    it("shows best-effort ghost when count_ matches multiple stats functions", () => {
      expect(getLogsQLGhostText("* | stats count_")).toBe("empty(<fields>) if (<filter>) as <name>");
    });

    it("shows running_stats function ghost text", () => {
      expect(getLogsQLGhostText("* | running_stats s")).toBe("um(<fields>) as <name>");
    });

    it("shows total_stats function ghost text", () => {
      expect(getLogsQLGhostText("* | total_stats s")).toBe("um(<fields>) as <name>");
    });

    it("returns to the parent stats syntax after a complete stats function", () => {
      expect(getLogsQLGhostText("* | stats count()")).toBe(" if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count(*)")).toBe(" if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count(\"a\", \"b\")")).toBe(" if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count() ")).toBe("if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count() as")).toBe(" <name>");
      expect(getLogsQLGhostText("* | stats by (host) count()")).toBe(" if (<filter>) as <name>");
    });
  });
});
