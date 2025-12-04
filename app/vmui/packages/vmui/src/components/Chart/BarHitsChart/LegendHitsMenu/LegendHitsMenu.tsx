import { FC } from "preact/compat";
import "./style.scss";
import { LegendLogHits, LegendLogHitsMenu } from "../../../../api/types";
import LegendHitsMenuStats from "./LegendHitsMenuStats";
import LegendHitsMenuBase from "./LegendHitsMenuBase";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import LegendHitsMenuFields from "./LegendHitsMenuFields";
import { LOGS_LIMIT_HITS } from "../../../../constants/logs";
import LegendHitsMenuVisibility from "./LegendHitsMenuVisibility";
import { ExtraFilter } from "../../../../pages/OverviewPage/FiltersBar/types";

const otherDescription = `Aggregated results for fields not in the top ${LOGS_LIMIT_HITS}`;

interface Props {
  legend: LegendLogHits;
  fields: string[];
  optionsVisibilitySection: LegendLogHitsMenu[];
  onApplyFilter: (value: ExtraFilter) => void;
  onClose: () => void;
}

const LegendHitsMenu: FC<Props> = ({ legend, fields, optionsVisibilitySection, onApplyFilter, onClose }) => {
  return (
    <div className="vm-legend-hits-menu">
      <LegendHitsMenuVisibility options={optionsVisibilitySection} />

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

      {legend.isOther && (
        <div className="vm-legend-hits-menu-section vm-legend-hits-menu-section_info">
          <LegendHitsMenuRow title={otherDescription}/>
        </div>
      )}
    </div>
  );
};

export default LegendHitsMenu;
