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
      iconStart: <CopyIcon/>,
      handler: handlerCopyLabel,
    },
    {
      title: `Add ${groupFieldHits} to filter`,
      iconStart: <FilterIcon/>,
      handler:  handleAddStreamToFilter(ExtraFilterOperator.Equals),
    },
    {
      title: `Exclude ${groupFieldHits} to filter`,
      iconStart: <FilterOffIcon/>,
      handler: handleAddStreamToFilter(ExtraFilterOperator.NotEquals),
    }
  ];

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

export default LegendHitsMenuBase;
