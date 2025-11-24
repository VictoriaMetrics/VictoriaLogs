import { useCallback, useState } from "preact/compat";
import { ErrorTypes, TimeParams } from "../../../types";
import { useTenant } from "../../../hooks/useTenant";
import { useAppState } from "../../../state/common/StateContext";
import dayjs from "dayjs";
import { getDurationFromPeriod, getTimeperiodForDuration } from "../../../utils/time";

type ResponseTimeRange = {
  start: string;
  end: string;
  hasTimeFilter: boolean;
}

interface ServerTimeParams extends TimeParams {
  hasTimeFilter: boolean;
}


export const useFetchQueryTime = (defaultQuery?: string) => {
  const { serverUrl } = useAppState();
  const tenant = useTenant();

  const [serverPeriod, setServerPeriod] = useState<ServerTimeParams | null>(null);
  const [isLoading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<ErrorTypes | string>();

  const fetchQueryTime = useCallback(async ({ period, query }: {
    period: TimeParams,
    query?: string
  }): Promise<ServerTimeParams | undefined> => {
    const params = new URLSearchParams({
      query: query || defaultQuery || "",
      start: period.start.toString(),
      end: period.end.toString(),
    });

    setServerPeriod(null);
    setLoading(true);
    setError("");

    try {
      const url = `${serverUrl}/select/logsql/query_time_range`;
      const response = await fetch(url, {
        method: "POST",
        headers: { ...tenant },
        body: params,
      });

      if (!response.ok || !response.body) {
        const text = await response.text();
        setError(text);
        setLoading(false);
        return;
      }

      const { start, end, hasTimeFilter }: ResponseTimeRange = await response.json();
      const startDate = dayjs(start);
      const endDate = dayjs(end);

      if (!startDate.isValid() || !endDate.isValid()) {
        const text = "Invalid date range";
        setError(text);
        setLoading(false);
        return;
      }

      const timeRange = { from: startDate.toDate(), to: endDate.toDate() };
      const durationPeriod = getDurationFromPeriod(timeRange);
      const period = getTimeperiodForDuration(durationPeriod, timeRange.to);
      const serverPeriod = { ...period, hasTimeFilter };
      setServerPeriod(serverPeriod);
      return serverPeriod;
    } catch (e) {
      if (e instanceof Error && e.name !== "AbortError") {
        setError(String(e));
        console.error(e);
      }
    } finally {
      setLoading(false);
    }
  }, [defaultQuery, serverUrl, tenant]);


  return {
    fetchQueryTime,
    serverPeriod,
    isLoading,
    error,
  };
};
