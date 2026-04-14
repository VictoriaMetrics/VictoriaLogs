import { useMemo } from "preact/compat";
import { AlignedData } from "uplot";

export type HitsChartAlert = {
  variant: "info" | "error";
  title: string;
  message: string;
} | null;

interface UseHitsChartAlertParams {
  data: AlignedData;
  error?: string;
  isLoading: boolean;
  hideChart: boolean;
}

export const useHitsChartAlert = ({
  data,
  error,
  isLoading,
  hideChart,
}: UseHitsChartAlertParams): HitsChartAlert => {
  return useMemo(() => {
    if (isLoading || hideChart) return null;

    if (error) {
      return {
        variant: "error",
        title: "Failed to load hits",
        message: error,
      };
    }

    const noData = data.every((d) => d.length === 0);
    if (noData) return null;

    const noTimestamps = data[0]?.length === 0;
    if (noTimestamps) {
      return {
        variant: "info",
        title: "No timestamps available",
        message: "No timestamp information available for the current queries and time range.",
      };
    }

    const noValues = data[1]?.length === 0;
    if (noValues) {
      return {
        variant: "info",
        title: "No hit values available",
        message: "No value information available for the current queries and time range.",
      };
    }

    return null;
  }, [data, error, isLoading, hideChart]);
};
