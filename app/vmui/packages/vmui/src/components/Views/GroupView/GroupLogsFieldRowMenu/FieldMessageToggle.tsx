import { FC } from "preact/compat";
import { VisibilityIcon, VisibilityOffIcon } from "../../../Main/Icons";
import { LOGS_URL_PARAMS } from "../../../../constants/logs";
import { useSearchParams } from "react-router-dom";

type Props = {
  field: string;
  onClose: () => void;
}

const FieldMessageToggle: FC<Props> = ({ field, onClose }) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const displayFieldsString = searchParams.get(LOGS_URL_PARAMS.DISPLAY_FIELDS) || "";
  const displayFields = displayFieldsString ? displayFieldsString.split(",") : [];
  const isSelectedField = displayFields.includes(field);

  const handleSelectDisplayField = () => {
    const prev = displayFields;
    const newDisplayFields = prev.includes(field) ? prev.filter(v => v !== field) : [...prev, field];
    searchParams.set(LOGS_URL_PARAMS.DISPLAY_FIELDS, newDisplayFields.join(","));
    setSearchParams(searchParams);
    onClose();
  };

  return (
    <div
      className="vm-legend-hits-menu-row vm-legend-hits-menu-row_interactive"
      onClick={handleSelectDisplayField}
    >
      <div className="vm-legend-hits-menu-row__icon">
        {isSelectedField ? <VisibilityOffIcon/> : <VisibilityIcon/>}
      </div>
      <div className="vm-legend-hits-menu-row__title">
        {isSelectedField ? "Restore original message" : "Show as message"}
      </div>
    </div>
  );
};

export default FieldMessageToggle;
