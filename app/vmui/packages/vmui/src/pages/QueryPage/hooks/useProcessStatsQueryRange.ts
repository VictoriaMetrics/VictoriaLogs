import { LOGS_LIMIT_HITS } from "../../../constants/logs";
import { LogHits, MetricResult } from "../../../api/types";
import { buildMetricLabel, promValueToNumber } from "../../../utils/metric";

type Props = {
  setError: (error: string) => void;
  setLogHits: (logHits: LogHits[]) => void;
}

export type ResponseMatrix = {
  status: string;
  data?: {
    result: MetricResult[];
    resultType: string;
  }
}

const useProcessStatsQueryRange = ({ setLogHits, setError }:Props) => {
  return (data: ResponseMatrix, fieldsLimit: number) => {
    const result: LogHits[] = [];
    const series = data?.data?.result;
    const seriesLen = series?.length;
    const limit = Number.isFinite(fieldsLimit) ? fieldsLimit : LOGS_LIMIT_HITS;

    if (!series) {
      setError("Error: No 'result' field in response");
      setLogHits([]);
      return result;
    }

    if (!seriesLen) {
      setLogHits([]);
      return result;
    }

    // calculate the total for each series
    const metaTotals: { idx: number; total: number }[] = new Array(seriesLen);

    for (let idx = 0; idx < seriesLen; idx++) {
      const s = series[idx];
      let total = 0;

      for (let j = 0; j < s.values.length; j++) {
        const [, valueStr] = s.values[j];
        total += promValueToNumber(valueStr);
      }

      metaTotals[idx] = { idx, total };
    }

    // sort by total for find top series
    metaTotals.sort((a, b) => b.total - a.total);

    // find Top and Other series
    const topCount = Math.min(limit, seriesLen);
    const topMetaTotals = metaTotals.slice(0, topCount);
    const restMetaTotals = metaTotals.slice(topCount);

    // aggregate other series
    const valuesByTs = new Map<number, number>();
    let otherTotal = 0;

    for (let i = 0; i < restMetaTotals.length; i++) {
      const seriesIdx = restMetaTotals[i].idx;
      const s = series[seriesIdx];

      for (let j = 0; j < s.values.length; j++) {
        const [ts, v] = s.values[j];
        const value = promValueToNumber(v);

        const prev = valuesByTs.get(ts) ?? 0;
        valuesByTs.set(ts, prev + value);

        otherTotal += value;
      }
    }

    if (restMetaTotals.length > 0) {
      const tsNumbers = Array.from(valuesByTs.keys()).sort((a, b) => a - b);
      const otherTimestamps = tsNumbers.map((ts) => new Date(ts * 1000).toISOString());
      const otherValues = tsNumbers.map((tsSec) => valuesByTs.get(tsSec) ?? 0);
      const otherSeries: LogHits = {
        timestamps: otherTimestamps,
        values: otherValues,
        total: otherTotal,
        fields: {},
        _isOther: true,
      };

      // other should be in start
      result.push(otherSeries);
    }

    for (let i = 0; i < topMetaTotals.length; i++) {
      const seriesIdx = topMetaTotals[i].idx;
      const s = series[seriesIdx];

      const len = s.values.length;
      const timestamps: string[] = new Array(len);
      const values: number[] = new Array(len);

      for (let j = 0; j < len; j++) {
        const [ts, v] = s.values[j];
        timestamps[j] = new Date(ts * 1000).toISOString();
        values[j] = promValueToNumber(v);
      }

      result.push({
        timestamps,
        values,
        total: topMetaTotals[i].total,
        fields: { name: buildMetricLabel(s.metric) },
        _isOther: false,
      });
    }

    setLogHits(result);
    return result;
  };
};

export default useProcessStatsQueryRange;
