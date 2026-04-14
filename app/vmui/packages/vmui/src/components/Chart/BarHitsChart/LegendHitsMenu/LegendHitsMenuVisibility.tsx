import { FC } from "preact/compat";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import { LegendLogHitsMenu } from "../../../../api/types";

export interface LegendMenuVisibilityProps {
  options: LegendLogHitsMenu[]
}

const LegendHitsMenuVisibility: FC<LegendMenuVisibilityProps> = ({ options }) => {
  return (
    <div className="vm-legend-hits-menu-section">
      {options.map(({ ...menuProps }) => (
        <LegendHitsMenuRow
          key={menuProps.title}
          {...menuProps}
        />
      ))}
    </div>
  );
};

export default LegendHitsMenuVisibility;
