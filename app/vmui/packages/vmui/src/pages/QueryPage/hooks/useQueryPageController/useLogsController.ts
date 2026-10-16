import { FetchLogsParams, useFetchLogs } from "../useFetchLogs";
import { useLimitController } from "../../../../components/Configurators/LogsLimitController/hooks/useLimitController";
import { useRef } from "preact/compat";

export const useLogsController = () => {
  const { limit } = useLimitController();

  const limitRef = useRef(limit);
  limitRef.current = limit;

  const { fetchLogs, ...logsRequestState } = useFetchLogs();

  const runLogs = async (params: FetchLogsParams) => {
    logsRequestState.abort();
    return fetchLogs({ ...params, limit: limitRef.current });
  };

  return {
    runLogs,
    ...logsRequestState,
  };
};
