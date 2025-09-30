import { FC, useEffect } from "preact/compat";
import FiltersBar from "./FiltersBar/FiltersBar";
import FiltersBarPreview from "./FiltersBar/FiltersBarPreview";
import { useTimeState } from "../../state/time/TimeStateContext";
import useSearchParamsFromObject from "../../hooks/useSearchParamsFromObject";
import TotalsSection from "./Totals/TotalsSection";
import OverviewHits from "./OverviewHits/OverviewHits";
import OverviewFields from "./OverviewFields/OverviewFields";
import OverviewLogs from "./OverviewLogs/OverviewLogs";
import "./style.scss";

const OverviewPage: FC = () => {
  const { duration, relativeTime, period } = useTimeState();
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();

  useEffect(() => {
    setSearchParamsFromKeys({
      "g0.range_input": duration,
      "g0.end_input": period.date,
      "g0.relative_time": relativeTime || "none",
    });
  }, [duration, period.date, relativeTime]);

  return (
    <div className="vm-explorer-page">
      <FiltersBar/>
      <TotalsSection/>
      <OverviewHits/>
      <OverviewFields/>
      <FiltersBarPreview/>
      <OverviewLogs/>
    </div>
  );
};

export default OverviewPage;
