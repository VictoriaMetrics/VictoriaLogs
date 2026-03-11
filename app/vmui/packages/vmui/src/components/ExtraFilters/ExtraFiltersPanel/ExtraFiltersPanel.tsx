import { FC, useMemo } from "preact/compat";
import { CloseIcon } from "../../Main/Icons";
import { ExtraFilter } from "../types";
import { escapeForLogsQLString } from "../../../utils/regexp";
import "./style.scss";

type Props = {
  extraFilters: ExtraFilter[];
  onRemove: (field: string, value: string) => void;
}

const ExtraFiltersPanel: FC<Props> = ({ extraFilters, onRemove }) => {
  const handleRemove = (filter: ExtraFilter) => () => {
    onRemove(filter.field, filter.value);
  };

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
        <div
          key={filter.label}
          className="vm-extra-filters-panel-item"
        >
          <div>
            {filter.label}
          </div>
          <div
            className="vm-extra-filters-panel-item__remove"
            onClick={handleRemove(filter)}
          >
            <CloseIcon/>
          </div>
        </div>
        ))}
    </div>
  );
};

export default ExtraFiltersPanel;
