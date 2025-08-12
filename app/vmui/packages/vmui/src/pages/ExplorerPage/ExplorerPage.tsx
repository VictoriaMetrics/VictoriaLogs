import { FC, useEffect } from "preact/compat";
import FiltersBar from "./FiltersBar/FiltersBar";
import { useTimeState } from "../../state/time/TimeStateContext";
import useSearchParamsFromObject from "../../hooks/useSearchParamsFromObject";
import "./style.scss";
import TotalsSection from "./Totals/TotalsSection";

const ExplorerPage: FC = () => {
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
    </div>
  );
};

export default ExplorerPage;
