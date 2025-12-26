import { render, screen } from "@testing-library/preact";
import { describe, expect, it } from "vitest";
import TableLogs from "./TableLogs";

const logs = [
  { _time: "2025-01-01T00:00:00Z", file: "a.go", _msg: "first message" },
  { _time: "2025-01-01T00:01:00Z", file: "b.go", _msg: "second message" },
];

describe("TableLogs", () => {
  it("renders columns in the provided display order", () => {
    render(
      <TableLogs
        logs={logs}
        displayColumns={["_msg", "file", "_time"]}
        tableCompact={false}
        columns={["_time", "file", "_msg"]}
        rowsPerPage={10}
      />
    );

    const headers = screen
      .getAllByRole("columnheader")
      .map((header) => header.textContent?.trim() || "");

    expect(headers.slice(0, 3)).toEqual(["_msg", "file", "_time"]);
  });
});
