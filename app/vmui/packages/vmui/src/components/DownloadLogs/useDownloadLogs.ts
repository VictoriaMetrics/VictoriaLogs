import { useCallback, useState } from "preact/compat";
import { formatDateToUTC } from "../../utils/time";
import dayjs from "dayjs";
import { TimeParams } from "../../types";
import { useFetchLogs } from "../../pages/QueryPage/hooks/useFetchLogs";
import showSaveFilePicker from "../../utils/nativeFileSystemAdapter/showSaveFilePicker";

type DownloadLog = {
  filename: string;
  queryParams?: Record<string, string>;
}

const useDownloadLogs = () => {
  const { isLoading: isFetchingLogs, error: fetchLogsError, fetchLogs } = useFetchLogs();

  const [saveFileError, setSaveFileError] = useState<string | null>(null);
  const [isSavingFile, setSavingFile] = useState(false);

  const isLoading = isFetchingLogs || isSavingFile;
  const error = fetchLogsError || saveFileError;

  const downloadLogs = useCallback(async ({ filename, queryParams, }: DownloadLog) => {
    setSaveFileError(null);

    const { query, start, end } = queryParams || {};
    const missing = ["query", "start", "end"].filter(k => !({ query, start, end })[k]);
    if (missing.length) {
      setSaveFileError(`Download failed: missing required params: ${missing.join(", ")}`);
      return false;
    }

    setSavingFile(true);
    try {
      const period: TimeParams = {
        start: dayjs(start).unix(),
        end: dayjs(end).unix(),
        date: formatDateToUTC(dayjs(end).toDate()),
      };

      const res = await fetchLogs({ query, period, isDownload: true });

      if (!res || Array.isArray(res) || !res.body) {
        setSaveFileError("Download failed: missing response body.");
        return false;
      }

      const handle = await showSaveFilePicker({ suggestedName: filename });
      const writable = await handle.createWritable();

      await res.body.pipeTo(writable, { preventCancel: true });

      return true;
    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError") {
        setSaveFileError(String(e));
        console.error(e);
      }
      return false;
    } finally {
      setSavingFile(false);
    }
  }, [fetchLogs]);

  return {
    error,
    isLoading,
    downloadLogs,
  };
};

export default useDownloadLogs;
