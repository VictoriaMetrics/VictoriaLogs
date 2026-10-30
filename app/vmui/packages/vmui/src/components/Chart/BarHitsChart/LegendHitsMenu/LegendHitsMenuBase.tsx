import { FC } from "preact/compat";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { CopyIcon, FilterIcon, FilterOffIcon } from "../../../Main/Icons";
import { LegendLogHits, LegendLogHitsMenu } from "../../../../api/types";
import { ExtraFilter, ExtraFilterOperator } from "../../../ExtraFilters/types";
import { useHitsChartConfig } from "../../../../pages/QueryPage/HitsPanel/hooks/useHitsChartConfig";

interface Props {
  legend: LegendLogHits;
  onApplyFilter: (value: ExtraFilter) => void;
  onClose: () => void;
}

const LegendHitsMenuBase: FC<Props> = ({ legend, onApplyFilter, onClose }) => {
  const copyToClipboard = useCopyToClipboard();
  const {
    groupFieldHits: { value: groupFieldHits },
  } = useHitsChartConfig();

  const singleGroupField = groupFieldHits.length === 1 ? groupFieldHits[0] : "";

  const handleAddStreamToFilter = (operator: ExtraFilterOperator) => () => {
    onApplyFilter({
      field: singleGroupField,
      value: legend.fields[singleGroupField] ?? legend.label,
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
      title: singleGroupField ? `Copy ${singleGroupField} name` : "Copy series name",
      iconStart: <CopyIcon/>,
      handler: handlerCopyLabel,
    },
    ...(singleGroupField ? [
      {
        title: `Add ${singleGroupField} to filter`,
        iconStart: <FilterIcon/>,
        handler:  handleAddStreamToFilter(ExtraFilterOperator.Equals),
      },
      {
        title: `Exclude ${singleGroupField} to filter`,
        iconStart: <FilterOffIcon/>,
        handler: handleAddStreamToFilter(ExtraFilterOperator.NotEquals),
      }
    ] : []),
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
