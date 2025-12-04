import { FC, useMemo } from "preact/compat";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import { CopyIcon, FilterIcon, FilterOffIcon } from "../../../Main/Icons";
import { convertToFieldFilter } from "../../../../utils/logs";
import { LegendLogHitsMenu } from "../../../../api/types";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { ExtraFilter, ExtraFilterOperator } from "../../../../pages/OverviewPage/FiltersBar/types";
import { useHitsChartConfig } from "../../../../pages/QueryPage/HitsChart/hooks/useHitsChartConfig";

interface Props {
  fields: string[];
  onApplyFilter: (value: ExtraFilter) => void;
  onClose: () => void;
}

const unquote = (str: string) => {
  if (
    (str.startsWith("\"") && str.endsWith("\"")) ||
    (str.startsWith("'") && str.endsWith("'"))
  ) {
    return str.slice(1, -1);
  }
  return str;
};

const stringToFilter = (string: string) => {
  const [field, rawValue] = string.split(":").map(part => part.trim());
  const value = unquote(rawValue);
  return { field, value };
};

const LegendHitsMenuFields: FC<Props> = ({ fields, onApplyFilter, onClose }) => {
  const copyToClipboard = useCopyToClipboard();
  const { groupFieldHits } = useHitsChartConfig();

  const handleCopy = (field: string) => async () => {
    await copyToClipboard(field, `${field} has been copied`);
    onClose();
  };

  const handleAddToFilter = (field: string, operator: ExtraFilterOperator) => () => {
    onApplyFilter({ ...stringToFilter(field), operator });
    onClose();
  };

  const generateFieldMenu = (field: string): LegendLogHitsMenu[] => {
    return [
      {
        title: "Copy",
        iconStart: <CopyIcon/>,
        handler: handleCopy(field),
      },
      {
        title: "Add to filter",
        iconStart: <FilterIcon/>,
        handler: handleAddToFilter(field, ExtraFilterOperator.Equals),
      },
      {
        title: "Exclude to filter",
        iconStart: <FilterOffIcon/>,
        handler: handleAddToFilter(field, ExtraFilterOperator.NotEquals),
      }
    ];
  };

  const fieldsWithMenu: LegendLogHitsMenu[] = useMemo(() => {
    return fields.map(field => {
      const title = convertToFieldFilter(field, groupFieldHits);
      return {
        title,
        submenu: generateFieldMenu(title),
      };
    });
  }, [fields]);

  return (
    <div className="vm-legend-hits-menu-section">
      {fieldsWithMenu?.map((field) => (
        <LegendHitsMenuRow
          key={field.title}
          {...field}
        />
      ))}
    </div>
  );
};

export default LegendHitsMenuFields;
