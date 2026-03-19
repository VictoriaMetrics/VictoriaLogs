import { FC, useMemo } from "preact/compat";
import { ExtraFilter } from "../types";
import { escapeForLogsQLString } from "../../../utils/regexp";
import ExtraFiltersPanelItem from "./ExtraFiltersPanelItem";
import "./style.scss";

type Props = {
  extraFilters: ExtraFilter[];
  onRemove: (field: string, value: string) => void;
}

const ExtraFiltersPanel: FC<Props> = ({ extraFilters, onRemove }) => {
  const getLabel = (filter: ExtraFilter) => {
    const escapedValue = escapeForLogsQLString(filter.value);
    const expr = `${filter.field}${filter.operator}"${escapedValue}"`;
    return filter.isStream ? `{${expr}}` : expr;
  };

  const formattedFilters = useMemo(() => {
    return extraFilters.map(f => ({ ...f, label: getLabel(f) }));
  }, [extraFilters]);

  if (!formattedFilters.length) return null;

  return (
    <div className="vm-extra-filters-panel">
      {formattedFilters.map((filter) => (
        <ExtraFiltersPanelItem
          key={filter.label}
          filter={filter}
          onRemove={onRemove}
        />
        ))}
    </div>
  );
};

export default ExtraFiltersPanel;
