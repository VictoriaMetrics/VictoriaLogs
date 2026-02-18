import { FC } from "preact/compat";
import { LegendLogHits } from "../../../../api/types";
import { formatNumber } from "../../../../utils/number";

interface Props {
  legend: LegendLogHits;
}

const LegendHitsMenuStats: FC<Props> = ({ legend }) => {
  const totalFormatted = formatNumber(legend.total);
  const percentage = Math.round((legend.total / legend.totalHits) * 100);

  return (
    <div className="vm-legend-hits-menu-section">
      <div className="vm-legend-hits-menu-row">
        <div className="vm-legend-hits-menu-row__title">
          Total: {totalFormatted} ({percentage}%)
        </div>
      </div>
    </div>
  );
};

export default LegendHitsMenuStats;
