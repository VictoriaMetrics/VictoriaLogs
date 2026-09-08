import { useMemo } from "preact/compat";
import { GRAPH_QUERY_MODE } from "../../../../components/Chart/BarHitsChart/types";
import { useSearchParams } from "react-router-dom";
import { useHitsChartConfig } from "../../HitsPanel/hooks/useHitsChartConfig";

const graphQueryModes = new Set<string>(Object.values(GRAPH_QUERY_MODE));

const isValidMode = (value: string | null): value is GRAPH_QUERY_MODE => {
  return value !== null && graphQueryModes.has(value);
};

export const useHitsTriggers = () => {
  const [searchParams] = useSearchParams();

  const rawMode = searchParams.get("graph_mode");
  const graphQueryMode = isValidMode(rawMode) ? rawMode : GRAPH_QUERY_MODE.hits;

  const isChartHidden = searchParams.get("hide_chart") === "true";

  const {
    topHits: { value: topHits },
    groupFieldHits: { value: groupFieldHits },
    step: { value: step },
  } = useHitsChartConfig();

  return useMemo(() => ({
    graphQueryMode,
    isChartHidden,
    topHits,
    groupFieldHits,
    step
  }), [graphQueryMode, isChartHidden, topHits, groupFieldHits, step]);
};
