import { FC, useMemo } from "preact/compat";
import Select from "../../../../components/Main/Select/Select";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useFetchFieldValues } from "../hooks/useFetchFieldValues";
import { useExtraFilters } from "../hooks/useExtraFilters";

type Props = {
  value: string;
  field: string;
  onChange: (value: string) => void;
}

const SelectFieldValue: FC<Props> = ({ value, field, onChange }) => {
  const { period: { start, end } } = useTimeState();
  const { fetchFieldValues, fieldValues, loading, error } = useFetchFieldValues();
  const { extraParams } = useExtraFilters();

  const handleOpen = async (isOpen: boolean) => {
    if (isOpen && field) {
      await fetchFieldValues({ start, end, field, extraParams });
    }
  };

  const noOptionsText = useMemo(() => {
    if (loading) return "Loading...";
    if (error) return String(error);
    if (!field) return "Please select a field name first";
    return "No values found";
  }, [loading, error, field]);

  return (
    <Select
      value={value}
      list={fieldValues.map(f => f.value)}
      placeholder={!field ? "Select field name first" : "Select field value"}
      noOptionsText={noOptionsText}
      onChange={onChange}
      onOpen={handleOpen}
      searchable
    />
  );
};

export default SelectFieldValue;
