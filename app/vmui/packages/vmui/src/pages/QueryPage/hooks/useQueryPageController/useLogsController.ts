import { FetchLogsParams, useFetchLogs } from "../useFetchLogs";
import { useLimitController } from "../../../../components/Configurators/LogsLimitController/hooks/useLimitController";
import { useRef } from "preact/compat";

export const useLogsController = () => {
  const { limit } = useLimitController();

  const limitRef = useRef(limit);
  limitRef.current = limit;

  const { fetchLogs, ...logsRequestState } = useFetchLogs();

  const runLogs = async (params: FetchLogsParams): Promise<boolean> => {
    logsRequestState.abort();

    try {
      const isSuccess = await fetchLogs({ ...params, limit: limitRef.current });

      return Boolean(isSuccess);
    } catch {
      return false;
    }
  };

  return {
    runLogs,
    ...logsRequestState,
  };
};
