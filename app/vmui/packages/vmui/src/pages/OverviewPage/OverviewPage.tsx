import { FC } from "preact/compat";
import FiltersBar from "./FiltersBar/FiltersBar";
import FiltersBarPreview from "./FiltersBar/FiltersBarPreview";
import TotalsSection from "./Totals/TotalsSection";
import OverviewHits from "./OverviewHits/OverviewHits";
import OverviewFields from "./OverviewFields/OverviewFields";
import OverviewLogs from "./OverviewLogs/OverviewLogs";
import "./style.scss";
import { useTimePeriod } from "../QueryPage/hooks/useTimePeriod";

const OverviewPage: FC = () => {
  // Resolve the time range once, so the hits chart and the logs share it and the cached field names.
  const { period } = useTimePeriod();

  return (
    <div className="vm-explorer-page">
      <FiltersBar/>
      <TotalsSection/>
      <OverviewHits period={period}/>
      <OverviewFields/>
      <FiltersBarPreview/>
      <OverviewLogs period={period}/>
    </div>
  );
};

export default OverviewPage;
