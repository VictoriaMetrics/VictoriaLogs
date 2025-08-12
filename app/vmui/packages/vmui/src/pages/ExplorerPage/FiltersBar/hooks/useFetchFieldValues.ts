import { useState, useCallback } from "preact/hooks";
import { useAppState } from "../../../../state/common/StateContext";
import { LogsFiledValues } from "../../../../api/types";

interface FetchOptions {
  start: number;
  end: number;
  field: string;
  extraParams?: URLSearchParams;
}

export const useFetchFieldValues = () => {
  const { serverUrl } = useAppState();

  const [fieldValues, setFieldValues] = useState<LogsFiledValues[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | string>("");

  const fetchFieldValues = useCallback(async (options: FetchOptions): Promise<void> => {
    setLoading(true);
    setError("");

    try {
      const baseParams = new URLSearchParams({
        field: options.field,
        start: options.start.toString(),
        end: options.end.toString(),
        limit: "1000",
        query: "*" // TODO: Replace with actual query if needed
      });

      const params = new URLSearchParams([
        ...baseParams,
        ...(options.extraParams ?? [])
      ]);

      const url = `${serverUrl}/select/logsql/field_values?${params.toString()}`;
      const response = await fetch(url);

      if (!response.ok) {
        const errorResponse = await response.text();
        const error = `${response.status} ${response.statusText}: ${errorResponse}`;
        console.error(error);
        setError(error);
        return;
      }

      const data: {values: LogsFiledValues[]} = await response.json();
      setFieldValues(data.values);
    } catch (err) {
      console.error(err);
      setError(err as Error);
    } finally {
      setLoading(false);
    }
  }, [serverUrl]);

  return {
    fieldValues,
    loading,
    error,
    fetchFieldValues
  };
};
