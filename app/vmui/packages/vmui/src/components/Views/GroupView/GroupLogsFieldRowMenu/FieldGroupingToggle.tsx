import { FC } from "preact/compat";
import { StorageIcon } from "../../../Main/Icons";
import { useSearchParams } from "react-router-dom";
import { LOGS_GROUP_BY, LOGS_URL_PARAMS, WITHOUT_GROUPING } from "../../../../constants/logs";

type Props = {
  field: string;
  onClose: () => void;
}

const FieldGroupingToggle: FC<Props> = ({ field, onClose }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const groupBy = searchParams.get(LOGS_URL_PARAMS.GROUP_BY) || LOGS_GROUP_BY;

  const isGroupByField = groupBy === field;

  const handleSelectGroupBy = () => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev);
      newParams.set(LOGS_URL_PARAMS.GROUP_BY, isGroupByField ? WITHOUT_GROUPING : field);
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
        {isGroupByField ? "Clear grouping" : "Group by this field"}
      </div>
    </div>
  );
};

export default FieldGroupingToggle;
