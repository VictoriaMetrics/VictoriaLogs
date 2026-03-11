import { useState, useCallback } from "preact/hooks";
import { useAppState } from "../../../state/common/StateContext";
import { LogsFieldValues } from "../../../api/types";
import { useOverviewDispatch, useOverviewState } from "../../../state/overview/OverviewStateContext";
import { useTenant } from "../../../hooks/useTenant";

interface FetchOptions {
  start: number;
  end: number;
  query?: string;
  extraParams?: URLSearchParams;
  skipNoiseFields?: boolean;
  skipStreamFields?: boolean;
}

const NOISE_FIELDS = ["_msg", "_time"];
const STREAM_FIELDS = ["_stream", "_stream_id"];

export const useFetchFieldNames = () => {
  const { serverUrl } = useAppState();
  const { fieldNames: fieldNamesState, fieldNamesParamsKey } = useOverviewState();
  const dispatch = useOverviewDispatch();
  const tenant = useTenant();

  const [fieldNames, setFieldNames] = useState<LogsFieldValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const setterFieldNames = (values: LogsFieldValues[], skipNoiseFields = true, skipStreamFields = false) => {
    const noiseFields = skipNoiseFields ? NOISE_FIELDS : [];
    const streamFields = skipStreamFields ? STREAM_FIELDS : [];
    const skipFields = noiseFields.concat(streamFields);
    const filteredFieldNames = !skipFields.length
      ? values
      : values.filter(v => !skipFields.includes(v.value));
    setFieldNames(filteredFieldNames);
  };

  const fetchFieldNames = useCallback(async (options: FetchOptions): Promise<void> => {
    setLoading(true);
    setError("");

    try {
      const baseParams = new URLSearchParams({
        start: options.start.toString(),
        end: options.end.toString(),
        query: options.query || "*"
      });

      const params = new URLSearchParams([
        ...baseParams,
        ...(options.extraParams ?? [])
      ]);
      const headers = { ...tenant };

      const queryParams = params.toString();
      const cacheKey = queryParams + JSON.stringify(tenant);

      if (fieldNamesParamsKey === cacheKey) {
        setterFieldNames(fieldNamesState, options.skipNoiseFields, options.skipStreamFields);
        setLoading(false);
        return;
      }

      const url = `${serverUrl}/select/logsql/field_names`;
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

      const data: { values: LogsFieldValues[] } = await response.json();
      setterFieldNames(data.values, options.skipNoiseFields, options.skipStreamFields);
      dispatch({
        type: "SET_FIELD_NAMES",
        payload: { rows: data.values, key: cacheKey }
      });
    } catch (err) {
      console.error(err);
      setError(err as Error);
    } finally {
      setLoading(false);
    }
  }, [serverUrl, tenant, fieldNamesState, fieldNamesParamsKey]);

  return {
    fieldNames,
    loading,
    error,
    fetchFieldNames
  };
};
