import { FC, useId, useMemo } from "preact/compat";
import RadioOption, { RadioOptionType } from "./RadioOption";
import "./style.scss";

type Props = {
  options: RadioOptionType[];
  value: string;
  onChange?: (option: RadioOptionType) => void;
  disabled?: boolean;
};

const RadioGroup: FC<Props> = ({ options, value, onChange, disabled = false }) => {

  const internalName = useId();
  const groupName = `radiogroup-${internalName}`;

  const optionMap = useMemo(() => new Map(options.map((o) => [o.value, o])), [options]);

  const handleSelect = (val: string) => {
    const selectedOption = optionMap.get(val);
    if (!selectedOption) return;
    onChange?.(selectedOption);
  };

  return (
    <fieldset
      role="radiogroup"
      aria-disabled={disabled}
      className="vm-radio-group"
    >
      {options.map((option, idx) => (
        <RadioOption
          key={option.value}
          idx={idx}
          groupName={groupName}
          option={option}
          selected={value}
          disabled={disabled}
          onChange={handleSelect}
        />
      ))}
    </fieldset>
  );
};

export default RadioGroup;
