import { fireEvent, render, screen } from "@testing-library/preact";
import { createRef } from "preact";
import { ComponentChildren } from "preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import GlobalSettings, { GlobalSettingsHandle } from "./GlobalSettings";
import { LOGS_LIMIT_WARN_DISMISSED_KEY } from "../../../constants/logs";

vi.mock("../../../hooks/useDeviceDetect", () => ({
  default: () => ({ isMobile: false }),
  getIsMobile: () => false,
}));
vi.mock("./Timezones/TimezonesPicker", () => ({ default: () => null }));
vi.mock("./QueryTimeOverride/QueryTimeOverride", () => ({ default: () => null }));
vi.mock("../ThemeControl/ThemeControl", () => ({ default: () => null }));
vi.mock("./BrowserTabController/BrowserTabController", () => ({ default: () => null }));
vi.mock("../../Main/Modal/Modal", () => ({
  default: ({ children }: { children: ComponentChildren }) => <>{children}</>,
}));

describe("GlobalSettings", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("re-enables large-load confirmation by clearing its saved preference", () => {
    localStorage.setItem(LOGS_LIMIT_WARN_DISMISSED_KEY, "true");
    const ref = createRef<GlobalSettingsHandle>();
    render(<GlobalSettings ref={ref}/>);

    fireEvent.click(screen.getByLabelText("settings"));
    fireEvent.click(screen.getByRole("button", { name: "Re-enable Confirm large load warning" }));

    expect(localStorage.getItem(LOGS_LIMIT_WARN_DISMISSED_KEY)).toBeNull();
  });
});
