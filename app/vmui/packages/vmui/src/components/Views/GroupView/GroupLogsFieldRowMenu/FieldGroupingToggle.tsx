import { FC } from "preact/compat";
import { StorageIcon } from "../../../Main/Icons";
import { useSearchParams } from "react-router-dom";
import { LOGS_GROUP_BY, LOGS_URL_PARAMS } from "../../../../constants/logs";
import { parseGroupByFields, serializeGroupByFields } from "../../../../utils/logs";

type Props = {
  field: string;
  onClose: () => void;
}

const FieldGroupingToggle: FC<Props> = ({ field, onClose }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const groupByFields = parseGroupByFields(searchParams.get(LOGS_URL_PARAMS.GROUP_BY) || LOGS_GROUP_BY);

  const isGroupByField = groupByFields.includes(field);
  const isOnlyGroupByField = isGroupByField && groupByFields.length === 1;

  const handleSelectGroupBy = () => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev);
      const next = isGroupByField ? groupByFields.filter(f => f !== field) : [...groupByFields, field];
      newParams.set(LOGS_URL_PARAMS.GROUP_BY, serializeGroupByFields(next));
      return newParams;
    });
    onClose();
  };

  return (
    <div
      className="vm-legend-hits-menu-row vm-legend-hits-menu-row_interactive"
      onClick={handleSelectGroupBy}
    >
      <div className="vm-legend-hits-menu-row__icon">{<StorageIcon/>}</div>
      <div className="vm-legend-hits-menu-row__title">
        {isOnlyGroupByField ? "Clear grouping" : isGroupByField ? "Remove from grouping" : "Group by this field"}
      </div>
    </div>
  );
};

export default FieldGroupingToggle;
