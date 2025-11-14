import { useState, useCallback } from "preact/hooks";
import { useAppState } from "../../../state/common/StateContext";
import { LogsFiledValues } from "../../../api/types";
import { useOverviewDispatch, useOverviewState } from "../../../state/overview/OverviewStateContext";
import { useTenant } from "../../../hooks/useTenant";

interface FetchOptions {
  start: number;
  end: number;
  query?: string;
  extraParams?: URLSearchParams;
}

export const useFetchStreamFieldNames = () => {
  const { serverUrl } = useAppState();
  const {
    streamsFieldNames: streamsFieldNamesState,
    streamsFieldNamesParamsKey
  } = useOverviewState();
  const dispatch = useOverviewDispatch();
  const tenant = useTenant();

  const [streamFieldNames, setStreamFieldNames] = useState<LogsFiledValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const fetchStreamFieldNames = useCallback(async (options: FetchOptions): Promise<void> => {
    setLoading(true);
    setError("");

    try {
      const baseParams = new URLSearchParams({
        start: options.start.toString(),
        end: options.end.toString(),
        query: options.query || "*",
      });

      const params = new URLSearchParams([
        ...baseParams,
        ...(options.extraParams ?? [])
      ]);
      const headers = { ...tenant };

      const queryParams = params.toString();
      const cacheKey = queryParams + JSON.stringify(tenant);

      if (streamsFieldNamesParamsKey === cacheKey) {
        setStreamFieldNames(streamsFieldNamesState);
        setLoading(false);
        return;
      }

      const url = `${serverUrl}/select/logsql/stream_field_names`;
      const response = await fetch(url, {
        method: "POST",
        headers,
        body: params,
      });

      if (!response.ok) {
        const errorResponse = await response.text();
        const error = `${response.status} ${response.statusText}: ${errorResponse}`;
        console.error(error);
        setError(error);
        return;
      }

      const data: { values: LogsFiledValues[] } = await response.json();
      setStreamFieldNames(data.values);
      dispatch({
        type: "SET_STREAM_FIELD_NAMES",
        payload: { rows: data.values, key: cacheKey }
      });
    } catch (err) {
      console.error(err);
      setError(err as Error);
    } finally {
      setLoading(false);
    }
  }, [serverUrl, tenant, streamsFieldNamesParamsKey, streamsFieldNamesState]);

  return {
    streamFieldNames,
    loading,
    error,
    fetchStreamFieldNames
  };
};
