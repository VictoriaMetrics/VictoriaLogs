import { FC, useCallback, useEffect, useMemo, useState } from "preact/compat";
import useBoolean from "../../hooks/useBoolean";
import { ReactNode } from "react";
import Modal from "../Main/Modal/Modal";
import "./style.scss";
import Button from "../Main/Button/Button";
import RadioGroup from "../Main/RadioGroup/RadioGroup";
import TextField from "../Main/TextField/TextField";
import dayjs from "dayjs";
import { DATE_TIME_FORMAT } from "../../constants/date";
import { Logs } from "../../api/types";
import { downloadCSV, downloadJSON, downloadJSONL } from "../../utils/file";
import { DownloadIcon, SpinnerIcon } from "../Main/Icons";
import Alert from "../Main/Alert/Alert";
import useDownloadLogs from "./useDownloadLogs";

type Props = {
  data?: Logs[];
  children: ReactNode;
  queryParams?: Record<string, string>;
};

enum Format {
  csv = "csv",
  json = "json",
  jsonl = "jsonl",
}

enum LimitOption {
  limit = "limit",
  unlimit = "unlimit",
}

const downloader = {
  [Format.jsonl]: (data: Logs[], filename: string) => {
    const jsonl = data.map(x => JSON.stringify(x)).join("\n");
    downloadJSONL(jsonl, filename);
  },
  [Format.json]: (data: Logs[], filename: string) => {
    const json = JSON.stringify(data, null, 2);
    downloadJSON(json, filename);
  },
  [Format.csv]: downloadCSV
};

const DownloadLogsModal: FC<Props> = ({ data, children, queryParams }) => {
  const {
    value: isOpen,
    setTrue: handleOpen,
    setFalse: handleClose,
  } = useBoolean(false);

  const { downloadLogs, error, isLoading } = useDownloadLogs();

  const [filename, setFilename] = useState("vmui_logs_export");
  const [fileExtension, setFileExtension] = useState(Format.jsonl);
  const [limitOption, setLimitOption] = useState(data ? LimitOption.limit : LimitOption.unlimit);

  const unlimitDisabled = fileExtension !== Format.jsonl;

  const queryContext = useMemo(() => {
    if (!queryParams) return [];

    const { start, end, limit, AccountID, ProjectID } = queryParams;

    const localeStart = start ? dayjs(start).format(DATE_TIME_FORMAT) : null;
    const localeEnd = end ? dayjs(end).format(DATE_TIME_FORMAT) : null;
    const tz = dayjs(start).format("Z");

    return [
      { label: "Time range", value: `${localeStart} - ${localeEnd} (${tz})` },
      { label: "Limit", value: limitOption === LimitOption.unlimit ? "" : limit },
      { label: "Tenant ID", value: `${AccountID}:${ProjectID}` },
    ].filter(item => item.value);
  }, [queryParams, limitOption]);

  const handleDownload = useCallback(async () => {
    const outName = `${filename}.${fileExtension}`;

    if (limitOption === LimitOption.unlimit) {
      await downloadLogs({ filename: outName, queryParams });
    }

    if (limitOption === LimitOption.limit && data) {
      const localDownloader = downloader[fileExtension];
      if (localDownloader) localDownloader(data, outName);
    }
  }, [filename, fileExtension, queryParams, limitOption, data, downloadLogs]);

  useEffect(() => {
    if (limitOption === LimitOption.unlimit && unlimitDisabled) {
      setLimitOption(LimitOption.limit);
    }
  }, [fileExtension]);

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
              <h3 className="vm-download-logs-section__title">
                File name
              </h3>

              <div className="vm-download-logs-filename">
                <TextField
                  autofocus
                  value={filename}
                  endIcon={<span className="vm-download-logs-filename__extension">.{fileExtension}</span>}
                  onChange={setFilename}
                />
              </div>
            </div>

            <div className="vm-download-logs-section">
              <h3 className="vm-download-logs-section__title">
                Format
              </h3>

              <RadioGroup
                value={fileExtension}
                onChange={(opt) => setFileExtension(opt.value as Format)}
                options={[
                  {
                    label: "JSONL",
                    value: Format.jsonl,
                    description: "Recommended. Exports visible rows or runs an unlimited query."
                  },
                  {
                    label: "JSON",
                    value: Format.json,
                    disabled: !data,
                    description: "Exports currently loaded rows as a JSON array."
                  },
                  {
                    label: "CSV",
                    value: Format.csv,
                    disabled: !data,
                    description: "Exports currently loaded rows as a CSV table."
                  },
                ]}
              />
            </div>

            <div className="vm-download-logs-section">
              <h3 className="vm-download-logs-section__title">
                Export scope
              </h3>

              <RadioGroup
                value={limitOption}
                onChange={(opt) => setLimitOption(opt.value as LimitOption)}
                options={[
                  {
                    label: "Visible results only",
                    value: LimitOption.limit,
                    description: "Export only the logs currently displayed in the UI.",
                    disabled: !data
                  },
                  {
                    label: "All logs (no limit)",
                    value: LimitOption.unlimit,
                    description: "Export all matching logs as JSONL.",
                    disabled: unlimitDisabled
                  }
                ]}
              />
            </div>

            <div className="vm-download-logs-section">
              <h3 className="vm-download-logs-section__title">
                Query context
              </h3>

              <div className="vm-download-logs-context">
                <div className="vm-download-logs-context-item">
                  <span className="vm-download-logs-context-item__label">LogsQL:</span>
                  <code className="vm-download-logs-context-item__query">{queryParams?.query}</code>
                </div>

                {queryContext.map(({ label, value }) => (
                  <div
                    key={label}
                    className="vm-download-logs-context-item"
                  >
                    <span className="vm-download-logs-context-item__label">{label}:</span>
                    <span className="vm-download-logs-context-item__value">{value}</span>
                  </div>
                ))}
              </div>
            </div>

            {error && <Alert variant="error">{error}</Alert>}

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
