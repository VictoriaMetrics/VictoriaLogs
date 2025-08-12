import { useState, useCallback } from "preact/hooks";
import { useAppState } from "../../../../state/common/StateContext";
import { LogsFiledValues } from "../../../../api/types";

interface FetchOptions {
  start: number;
  end: number;
  extraParams?: URLSearchParams;
}

const HIDE_FIELDS = ["_msg", "_time"];

export const useFetchFieldNames = () => {
  const { serverUrl } = useAppState();

  const [fieldNames, setFieldNames] = useState<LogsFiledValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const fetchFieldNames = useCallback(async (options: FetchOptions): Promise<void> => {
    setLoading(true);
    setError("");

    try {
      const baseParams = new URLSearchParams({
        start: options.start.toString(),
        end: options.end.toString(),
        limit: "1000",
        query: "*" // TODO: Replace with actual query if needed
      });

      const params = new URLSearchParams([
        ...baseParams,
        ...(options.extraParams ?? [])
      ]);

      const url = `${serverUrl}/select/logsql/field_names?${params.toString()}`;
      const response = await fetch(url);

      if (!response.ok) {
        const errorResponse = await response.text();
        const error = `${response.status} ${response.statusText}: ${errorResponse}`;
        console.error(error);
        setError(error);
        return;
      }

      const data: {values: LogsFiledValues[]} = await response.json();
      const fieldNames = data.values
        .filter(v => !HIDE_FIELDS.includes(v.value))
        .map(v => ({ ...v, icon: v.hits }));
      setFieldNames(fieldNames);
    } catch (err) {
      console.error(err);
      setError(err as Error);
    } finally {
      setLoading(false);
    }
  }, [serverUrl]);

  return {
    fieldNames,
    loading,
    error,
    fetchFieldNames
  };
};
