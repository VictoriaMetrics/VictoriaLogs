import { describe, it, expect, vi } from "vitest";
import { render, fireEvent, screen } from "@testing-library/preact";
import Table from "./Table";
import { Column } from "./types";

vi.mock("./hooks/useTableColumnPrefs", () => ({
  useTableColumnPrefs: () => ({
    getColumnPrefs: () => ({}),
    updateColumnPref: () => {
      /* no-op */
    },
  }),
}));

type Row = { _time: string; msg: string };

const columns: Column<Row>[] = [
  { key: "_time", title: "_time", options: { sortable: false, resizable: false, draggable: false, menuEnabled: false } },
  { key: "msg", title: "msg", options: { sortable: false, resizable: false, draggable: false, menuEnabled: false } },
];
const rows: Row[] = [{ _time: "1", msg: "hello" }];

describe("Table renderExpandedRow", () => {
  it("renders no expand controls without the prop", () => {
    render(<Table
      tableId="t"
      rows={rows}
      columns={columns}
      paginationOffset={[0, 10]}
    />);
    expect(screen.queryByLabelText("Expand row")).toBeNull();
  });

  it("expands a row on chevron click", () => {
    render(<Table
      tableId="t"
      rows={rows}
      columns={columns}
      paginationOffset={[0, 10]}
      renderExpandedRow={(row) => <div data-testid="expanded">{row.msg}-details</div>}
    />);
    expect(screen.queryByTestId("expanded")).toBeNull();

    const expandBtn = screen.getByLabelText("Expand row");
    expect(expandBtn.getAttribute("aria-expanded")).toBe("false");

    fireEvent.click(expandBtn);
    expect(screen.getByTestId("expanded")).toHaveTextContent("hello-details");

    const collapseBtn = screen.getByLabelText("Collapse row");
    expect(collapseBtn.getAttribute("aria-expanded")).toBe("true");

    fireEvent.click(collapseBtn);
    expect(screen.queryByTestId("expanded")).toBeNull();
    expect(screen.getByLabelText("Expand row").getAttribute("aria-expanded")).toBe("false");
  });
});
