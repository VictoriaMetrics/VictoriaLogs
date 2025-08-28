import { FC, useEffect } from "preact/compat";
import "./style.scss";
import { useFetchTotals } from "./hooks/useFetchTotals";
import Alert from "../../../components/Main/Alert/Alert";
import TotalCard from "./TotalCard";
import { explorerTotals } from "./totalsConfig";
import { useOverviewDispatch } from "../../../state/overview/OverviewStateContext";

const TotalsSection: FC = () => {
  const {
    totals,
    totalsPrev,
    isLoading,
    error,
    periods,
  } = useFetchTotals();

  const dispatch = useOverviewDispatch();

  useEffect(() => {
    dispatch({ type: "SET_TOTAL_LOGS", payload: Number(totals?.totalLogs || 0) });
  }, [totals]);

  return (
    <div className="vm-total-section">
      {explorerTotals.map(total => (
        <TotalCard
          {...total}
          key={total.title}
          periods={periods}
          isLoading={isLoading}
          value={totals?.[total.alias]}
          valuePrev={totalsPrev?.[total.alias]}
        />
      ))}
      {error && <Alert variant="error">{error}</Alert>}
    </div>
  );
};

export default TotalsSection;
