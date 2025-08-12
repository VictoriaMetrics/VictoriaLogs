import { FC } from "preact/compat";
import "./style.scss";
import { useFetchTotals } from "./hooks/useFetchTotals";
import Alert from "../../../components/Main/Alert/Alert";
import TotalCard from "./TotalCard";
import { explorerTotals } from "./totalsConfig";

const TotalsSection: FC = () => {
  const {
    logs,
    isLoading,
    error,
  } = useFetchTotals();

  const totals = logs[0] || {};

  return (
    <div className="vm-total-section">

      {explorerTotals.map(total => (
        <TotalCard
          {...total}
          key={total.title}
          isLoading={isLoading}
          value={totals[total.alias]}
        />
      ))}
      {error && <Alert variant="error">{error}</Alert>}
    </div>
  );
};

export default TotalsSection;
