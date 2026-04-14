import { FC, useCallback } from "preact/compat";
import "./style.scss";
import { LegendLogHits, LegendLogHitsMenu } from "../../../../api/types";
import LegendHitsMenuStats from "./LegendHitsMenuStats";
import LegendHitsMenuBase from "./LegendHitsMenuBase";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import LegendHitsMenuFields from "./LegendHitsMenuFields";
import { LOGS_LIMIT_HITS } from "../../../../constants/logs";
import LegendHitsMenuVisibility from "./LegendHitsMenuVisibility";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";
import { ExtraFilter } from "../../../ExtraFilters/types";

const otherDescription = `Aggregated results for fields not in the top ${LOGS_LIMIT_HITS}`;

interface Props {
  legend: LegendLogHits;
  fields: string[];
  optionsVisibilitySection: LegendLogHitsMenu[];
  onClose: () => void;
}

const LegendHitsMenu: FC<Props> = ({ legend, fields, optionsVisibilitySection, onClose }) => {
  const { addNewFilter } = useExtraFilters();

  const handleApplyFilter = useCallback((filter: ExtraFilter) => {
    addNewFilter(filter);
  }, [addNewFilter]);

  return (
    <div className="vm-legend-hits-menu">
      <LegendHitsMenuVisibility options={optionsVisibilitySection} />

      {!legend.isOther && (
        <LegendHitsMenuBase
          legend={legend}
          onApplyFilter={handleApplyFilter}
          onClose={onClose}
        />
      )}

      {!legend.isOther && (
        <LegendHitsMenuFields
          fields={fields}
          onApplyFilter={handleApplyFilter}
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
