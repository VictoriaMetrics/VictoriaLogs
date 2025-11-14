import { FC, useEffect, useState } from "preact/compat";
import Modal from "../../../components/Main/Modal/Modal";
import Button from "../../../components/Main/Button/Button";
import LogsLimitInput from "./LogsLimitInput";
import "./style.scss";
import Checkbox from "../../../components/Main/Checkbox/Checkbox";
import DownloadLogsModal from "../../../components/DownloadLogs/DownloadLogsModal";

type Props = {
  isOpen: boolean;
  initialLimit: number;
  limitDraft: number;
  setLimitDraft: (limit: number) => void;
  suppressWarning: boolean;
  queryParams?: Record<string, string>;
  onChangeSuppressWarning: (value: boolean) => void;
  onConfirm: () => void;
  onCancel: () => void;
}

const LimitConfirmModal: FC<Props> = ({
  initialLimit,
  limitDraft,
  setLimitDraft,
  isOpen,
  suppressWarning,
  queryParams,
  onChangeSuppressWarning,
  onConfirm,
  onCancel
}) => {
  const limitText = !initialLimit ? "an unlimited number of" : initialLimit.toLocaleString("en-US");
  const [error, setError] = useState(false);

  useEffect(() => () => onCancel(), [onCancel]);

  if (!isOpen) return null;

  return (
    <Modal
      title={"Confirm large load"}
      isOpen={isOpen}
      onClose={onCancel}
    >
      <div className="vm-logs-limit-modal">
        <div className="vm-logs-limit-modal-text">
          <p>You’re about to load <b>{limitText}</b> logs.</p>
          <p>This may slow down the app or make the UI unresponsive.</p>
          <p>Are you sure you want to continue?</p>
        </div>

        <div className="vm-logs-limit-modal-input">
          <LogsLimitInput
            limit={limitDraft}
            onChangeLimit={setLimitDraft}
            onPressEnter={onConfirm}
            onError={setError}
          />
        </div>

        <DownloadLogsModal queryParams={queryParams}>
          <p className="vm-logs-limit-modal-download vm-link vm-link_colored vm-link_underlined">
            Click to download all matching logs without a limit.
          </p>
        </DownloadLogsModal>

        <div className="vm-logs-limit-modal-footer">
          <div>
            <Checkbox
              color="primary"
              label="Don't show again in this tab"
              checked={suppressWarning}
              onChange={onChangeSuppressWarning}
            />

          </div>

          <div className="vm-logs-limit-modal-footer__actions">
            <Button
              color="error"
              variant="outlined"
              onClick={onCancel}
            >
              Cancel
            </Button>
            <Button
              onClick={onConfirm}
              disabled={error}
            >
              Load Logs
            </Button>
          </div>
        </div>
      </div>
    </Modal>
  );
};

export default LimitConfirmModal;
