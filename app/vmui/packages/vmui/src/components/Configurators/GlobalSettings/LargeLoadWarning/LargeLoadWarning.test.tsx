import { fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";
import LargeLoadWarning from "./LargeLoadWarning";

describe("LargeLoadWarning", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("persists dismissed warning when switch is turned off", () => {
    render(<LargeLoadWarning/>);

    fireEvent.click(screen.getByText("Show confirmation for large loads"));

    expect(localStorage.getItem("VLUI:LOGS_LIMIT_WARN_DISMISSED")).toBe(JSON.stringify({ value: true }));
  });
});
