import { FC } from "preact/compat";
import FiltersBar from "./FiltersBar/FiltersBar";
import FiltersBarPreview from "./FiltersBar/FiltersBarPreview";
import TotalsSection from "./Totals/TotalsSection";
import OverviewHits from "./OverviewHits/OverviewHits";
import OverviewFields from "./OverviewFields/OverviewFields";
import OverviewLogs from "./OverviewLogs/OverviewLogs";
import "./style.scss";

const OverviewPage: FC = () => {
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
