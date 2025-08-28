import { FC } from "preact/compat";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { CopyIcon, FilterIcon, FilterOffIcon } from "../../../Main/Icons";
import { LegendLogHits, LegendLogHitsMenu } from "../../../../api/types";
import { ExtraFilter, ExtraFilterOperator } from "../../../../pages/OverviewPage/FiltersBar/types";
import { useHitsChartConfig } from "../../../../pages/QueryPage/HitsChart/hooks/useHitsChartConfig";

interface Props {
  legend: LegendLogHits;
  onApplyFilter: (value: ExtraFilter) => void;
  onClose: () => void;
}

const LegendHitsMenuBase: FC<Props> = ({ legend, onApplyFilter, onClose }) => {
  const copyToClipboard = useCopyToClipboard();
  const { groupFieldHits } = useHitsChartConfig();

  const handleAddStreamToFilter = (operator: ExtraFilterOperator) => () => {
    onApplyFilter({
      field: groupFieldHits,
      value: legend.label,
      operator,
    });
    onClose();
  };

  const handlerCopyLabel = async () => {
    await copyToClipboard(legend.label, `${legend.label} has been copied`);
    onClose();
  };

  const options: LegendLogHitsMenu[] = [
    {
      title: `Copy ${groupFieldHits} name`,
      icon: <CopyIcon/>,
      handler: handlerCopyLabel,
    },
    {
      title: `Add ${groupFieldHits} to filter`,
      icon: <FilterIcon/>,
      handler:  handleAddStreamToFilter(ExtraFilterOperator.Equals),
    },
    {
      title: `Exclude ${groupFieldHits} to filter`,
      icon: <FilterOffIcon/>,
      handler: handleAddStreamToFilter(ExtraFilterOperator.NotEquals),
    }
  ];

  return (
    <div className="vm-legend-hits-menu-section">
      {options.map(({ icon, title, handler }) => (
        <LegendHitsMenuRow
          key={title}
          iconStart={icon}
          title={title}
          handler={handler}
        />
      ))}
    </div>
  );
};

export default LegendHitsMenuBase;
