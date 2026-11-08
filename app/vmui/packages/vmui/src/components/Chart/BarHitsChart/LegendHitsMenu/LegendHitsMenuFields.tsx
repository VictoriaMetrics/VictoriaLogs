import { FC, useMemo } from "preact/compat";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import { CopyIcon, FilterIcon, FilterOffIcon } from "../../../Main/Icons";
import { convertToFieldFilter, formatFieldPair } from "../../../../utils/logs";
import { LegendLogHitsMenu } from "../../../../api/types";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { ExtraFilter, ExtraFilterOperator } from "../../../ExtraFilters/types";
import { FieldPair } from "../../../../types";

interface Props {
  pairs: FieldPair[];
  onApplyFilter: (value: ExtraFilter) => void;
  onClose: () => void;
}

const LegendHitsMenuFields: FC<Props> = ({ pairs, onApplyFilter, onClose }) => {
  const copyToClipboard = useCopyToClipboard();

  const handleCopy = (title: string) => async () => {
    await copyToClipboard(title, `${title} has been copied`);
    onClose();
  };

  const handleAddToFilter = (pair: FieldPair, operator: ExtraFilterOperator) => () => {
    onApplyFilter({ field: pair.field, value: pair.value, operator, isStream: pair.isStream });
    onClose();
  };

  const generateFieldMenu = (pair: FieldPair, title: string): LegendLogHitsMenu[] => {
    return [
      {
        title: "Copy",
        iconStart: <CopyIcon/>,
        handler: handleCopy(title),
      },
      {
        title: "Add to filter",
        iconStart: <FilterIcon/>,
        handler: handleAddToFilter(pair, ExtraFilterOperator.Equals),
      },
      {
        title: "Exclude to filter",
        iconStart: <FilterOffIcon/>,
        handler: handleAddToFilter(pair, ExtraFilterOperator.NotEquals),
      }
    ];
  };

  const fieldsWithMenu: LegendLogHitsMenu[] = useMemo(() => {
    return pairs.map(pair => {
      const title = convertToFieldFilter(formatFieldPair(pair));
      return {
        title,
        submenu: generateFieldMenu(pair, title),
      };
    });
  }, [pairs]);

  return (
    <div className="vm-legend-hits-menu-section">
      {fieldsWithMenu?.map((field, i) => (
        <LegendHitsMenuRow
          key={`${i}_${field.title}`}
          {...field}
        />
      ))}
    </div>
  );
};

export default LegendHitsMenuFields;
