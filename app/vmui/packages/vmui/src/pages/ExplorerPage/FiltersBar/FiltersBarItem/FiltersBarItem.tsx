import { FC, useState } from "preact/compat";
import { ExtraFilter, ExtraFilterOperator } from "../types";
import Button from "../../../../components/Main/Button/Button";
import { CloseIcon } from "../../../../components/Main/Icons";
import SelectFieldName from "./SelectFieldName";
import SelectOperator from "./SelectOperator";
import SelectFieldValue from "./SelectFieldValue";

type Props = {
  defaultFilter: ExtraFilter;
  isEditMode: boolean;
  onEditMode: () => void;
  onChange: (filter: ExtraFilter) => void;
  onRemove: () => void;
}

const FiltersBarItem: FC<Props> = ({ defaultFilter, isEditMode, onEditMode, onChange, onRemove }) => {
  const [filter, setFilter] = useState<ExtraFilter>({
    field: defaultFilter.field || "",
    operator: defaultFilter.operator || ExtraFilterOperator.Equals,
    value: defaultFilter.value || "",
  });

  const handleChangeField = (key: string, value: string) => {
    setFilter(prev => {
      const newFilter = { ...prev, [key]: value };
      onChange(newFilter);
      return newFilter;
    });
  };

  if (!isEditMode) {
    return (
      <div
        className="vm-filters-bar-body-item vm-filters-bar-body-item_readonly"
        onClick={onEditMode}
      >
        {filter.field} {filter.operator} {filter.value}
      </div>
    );
  }

  return (
    <div className="vm-filters-bar-body-item">
      <SelectFieldName
        value={filter.field}
        onChange={(value: string) => handleChangeField("field", value)}
      />
      <SelectOperator
        value={filter.operator}
        onChange={(value: string) => handleChangeField("operator", value)}
      />
      <SelectFieldValue
        value={filter.value}
        field={filter.field}
        onChange={(value: string) => handleChangeField("value", value)}
      />
      <div className="vm-filters-bar-body-item-actions">
        <Button
          size="small"
          color="gray"
          variant="text"
          startIcon={<CloseIcon/>}
          onClick={onRemove}
        />
      </div>
    </div>
  );
};

export default FiltersBarItem;
