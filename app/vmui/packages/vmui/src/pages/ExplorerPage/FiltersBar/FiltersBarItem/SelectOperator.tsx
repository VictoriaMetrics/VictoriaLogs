import { FC } from "preact/compat";
import Select from "../../../../components/Main/Select/Select";
import { ExtraFilterOperator } from "../types";

const operators = Object.values(ExtraFilterOperator);

type Props = {
  value: ExtraFilterOperator;
  onChange: (value: ExtraFilterOperator) => void;
}

const SelectOperator: FC<Props> = ({ value, onChange }) => {
  const handleChange = (value: string) => {
    onChange(value as ExtraFilterOperator);
  };

  return (
    <Select
      value={value}
      list={operators}
      onChange={handleChange}
    />
  );
};

export default SelectOperator;
