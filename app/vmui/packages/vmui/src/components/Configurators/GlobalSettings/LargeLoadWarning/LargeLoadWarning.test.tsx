import { fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";
import { getFromStorage } from "../../../../utils/storage";
import LargeLoadWarning from "./LargeLoadWarning";

describe("LargeLoadWarning", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("persists dismissed warning when switch is turned off", () => {
    const { container } = render(<LargeLoadWarning/>);

    fireEvent.click(screen.getByText("Show confirmation for large loads"));

    expect(getFromStorage("LOGS_LIMIT_WARN_DISMISSED")).toBe(true);
    expect(container.querySelector(".vm-switch_active")).toBeNull();
  });
});
