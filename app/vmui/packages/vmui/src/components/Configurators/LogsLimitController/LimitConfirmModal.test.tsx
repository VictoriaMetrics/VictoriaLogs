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
  it("enables permanent dismissal only when warning dismissal is selected", () => {
    const onChangePersistWarning = vi.fn();
    const props = {
      isOpen: true,
      initialLimit: 5000,
      limitDraft: 5000,
      setLimitDraft: vi.fn(),
      suppressWarning: false,
      persistWarning: false,
      onChangeSuppressWarning: vi.fn(),
      onChangePersistWarning,
      onConfirm: vi.fn(),
      onCancel: vi.fn(),
    };
    const { rerender } = render(<LimitConfirmModal {...props}/>);

    const permanentLabel = screen.getByText("Remember permanently");
    fireEvent.click(permanentLabel);
    expect(onChangePersistWarning).not.toHaveBeenCalled();

    rerender(<LimitConfirmModal
      {...props}
      suppressWarning
    />);
    fireEvent.click(screen.getByText("Remember permanently"));
    expect(onChangePersistWarning).toHaveBeenCalledWith(true);
  });
});
