import { fireEvent, render, screen } from "@testing-library/preact";
import { ComponentChildren } from "preact";
import { describe, expect, it, vi } from "vitest";
import LimitConfirmModal from "./LimitConfirmModal";

vi.mock("../../Main/Modal/Modal", () => ({
  default: ({ children }: { children: ComponentChildren }) => <>{children}</>,
}));

vi.mock("../../DownloadLogs/DownloadLogsModal", () => ({
  default: ({ children }: { children: ComponentChildren }) => <>{children}</>,
}));

vi.mock("./LogsLimitInput", () => ({
  default: () => null,
}));

describe("LimitConfirmModal", () => {
  it("offers persistent warning dismissal", () => {
    const onChangeSuppressWarning = vi.fn();
    const props = {
      isOpen: true,
      initialLimit: 5000,
      limitDraft: 5000,
      setLimitDraft: vi.fn(),
      suppressWarning: false,
      onChangeSuppressWarning,
      onConfirm: vi.fn(),
      onCancel: vi.fn(),
    };
    render(<LimitConfirmModal {...props}/>);

    fireEvent.click(screen.getByText("Don't show again"));
    expect(onChangeSuppressWarning).toHaveBeenCalledWith(true);
  });
});
