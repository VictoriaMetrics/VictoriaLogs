import { FC } from "preact/compat";
import { useFetchFieldNames } from "../hooks/useFetchFieldNames";
import Select from "../../../../components/Main/Select/Select";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useExtraFilters } from "../hooks/useExtraFilters";

type Props = {
  value: string;
  onChange: (value: string) => void;
}

const SelectFieldName: FC<Props> = ({ value, onChange }) => {
  const { period: { start, end } } = useTimeState();
  const { fetchFieldNames, fieldNames, loading, error } = useFetchFieldNames();
  const { extraParams } = useExtraFilters();

  const handleOpen = async (isOpen: boolean) => {
    if (isOpen && fieldNames.length === 0) {
      await fetchFieldNames({ start, end, extraParams });
    }
  };

  return (
    <Select
      value={value}
      list={fieldNames.map(f => f.value)}
      placeholder="Select field name"
      noOptionsText={loading ? "Loading..." : (`${error}` || "No field names found")}
      onChange={onChange}
      onOpen={handleOpen}
      searchable
    />
  );
};

export default SelectFieldName;
