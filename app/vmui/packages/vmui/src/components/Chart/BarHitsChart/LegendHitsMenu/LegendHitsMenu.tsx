import { FC } from "preact/compat";
import { Series } from "uplot";
import "./style.scss";
import { LegendLogHits } from "../../../../api/types";
import LegendHitsMenuStats from "./LegendHitsMenuStats";
import LegendHitsMenuBase from "./LegendHitsMenuBase";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import LegendHitsMenuFields from "./LegendHitsMenuFields";
import { LOGS_LIMIT_HITS } from "../../../../constants/logs";
import LegendHitsMenuVisibility from "./LegendHitsMenuVisibility";

const otherDescription = `aggregated results for fields not in the top ${LOGS_LIMIT_HITS}`;

interface Props {
  legend: LegendLogHits;
  fields: string[];
  series: Series[];
  onApplyFilter: (value: string) => void;
  onRedrawGraph: () => void;
  onClose: () => void;
}

const LegendHitsMenu: FC<Props> = ({ legend, fields, series, onApplyFilter, onRedrawGraph, onClose }) => {
  return (
    <div className="vm-legend-hits-menu">
      {legend.isOther && (
        <div className="vm-legend-hits-menu-section vm-legend-hits-menu-section_info">
          <LegendHitsMenuRow title={otherDescription}/>
        </div>
      )}

      <LegendHitsMenuVisibility
        legend={legend}
        series={series}
        onRedrawGraph={onRedrawGraph}
        onClose={onClose}
      />

      {!legend.isOther && (
        <LegendHitsMenuBase
          legend={legend}
          onApplyFilter={onApplyFilter}
          onClose={onClose}
        />
      )}

      {!legend.isOther && (
        <LegendHitsMenuFields
          fields={fields}
          onApplyFilter={onApplyFilter}
          onClose={onClose}
        />
      )}

      <LegendHitsMenuStats legend={legend}/>
    </div>
  );
};

export default LegendHitsMenu;
