import { FC, useCallback, useEffect, useMemo, useState } from "preact/compat";
import useBoolean from "../../hooks/useBoolean";
import { ReactNode } from "react";
import Modal from "../Main/Modal/Modal";
import "./style.scss";
import Button from "../Main/Button/Button";
import TextField from "../Main/TextField/TextField";
import dayjs from "dayjs";
import { DATE_TIME_FORMAT } from "../../constants/date";
import { DownloadIcon, SpinnerIcon } from "../Main/Icons";
import Alert from "../Main/Alert/Alert";
import useDownloadLogs from "./useDownloadLogs";
import Select from "../Main/Select/Select";

type Props = {
  children: ReactNode;
  queryParams?: Record<string, string>;
};

const extensions = ["jsonl", "csv"];

const DownloadLogsModal: FC<Props> = ({ children, queryParams }) => {
  const {
    value: isOpen,
    setTrue: handleOpen,
    setFalse: handleClose,
  } = useBoolean(false);

  const { downloadLogs, error, setError, isLoading } = useDownloadLogs();

  const [filename, setFilename] = useState("vmui_logs_export");
  const [fileExtension, setFileExtension] = useState("jsonl");

  const period = useMemo(() => {
    if (!queryParams) return "";
    const { start, end } = queryParams;
    const localeStart = start ? dayjs(start).format(DATE_TIME_FORMAT) : "";
    const localeEnd = end ? dayjs(end).format(DATE_TIME_FORMAT) : "";
    const tz = dayjs(start).format("Z");
    return `${localeStart} - ${localeEnd} (${tz})`;
  }, [queryParams?.start, queryParams?.end]);

  const tenant = useMemo(() => {
    if (!queryParams) return "";
    const { AccountID, ProjectID } = queryParams;
    return `${AccountID}:${ProjectID}`;
  }, [queryParams?.AccountID, queryParams?.ProjectID]);

  const handleDownload = useCallback(async () => {
    const safeName = filename.trim() || "vmui_logs_export";
    const outName = `${safeName}.${fileExtension}`;

    await downloadLogs({ filename: outName, queryParams: { ...queryParams, format: fileExtension } });
  }, [filename, fileExtension, queryParams, downloadLogs]);

  useEffect(() => {
    if (!isOpen) {
      setError(null);
    }
  }, [isOpen]);

  return (
    <>
      <div onClick={handleOpen}>
        {children}
      </div>

      {isOpen && (
        <Modal
          title="Download logs"
          onClose={handleClose}
        >
          <div className="vm-download-logs">
            <div className="vm-download-logs-section">
              <div className="vm-download-logs-filename">
                <TextField
                  label="File name"
                  autofocus
                  value={filename}
                  onChange={setFilename}
                />
              </div>
              <div className="vm-download-logs-extension">
                <Select
                  label="Format"
                  value={fileExtension}
                  list={extensions}
                  onChange={setFileExtension}
                />
              </div>

              <div className="vm-download-logs__description">
                This will download all logs for {period} using your current query for tenant {tenant}.
              </div>
            </div>

            {error && (
              <Alert
                title="Download failed"
                variant="error"
              >
                {error}
              </Alert>
            )}

            <div className="vm-download-logs-footer">
              <Button
                color="error"
                variant="outlined"
                onClick={handleClose}
              >
                Cancel
              </Button>

              <Button
                color="primary"
                variant="contained"
                onClick={handleDownload}
                disabled={isLoading}
                startIcon={isLoading ? <SpinnerIcon/> : <DownloadIcon/>}
              >
                Download
              </Button>
            </div>
          </div>
        </Modal>
      )}
    </>
  );
};

export default DownloadLogsModal;
