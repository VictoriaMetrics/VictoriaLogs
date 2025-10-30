import { FC } from "preact/compat";

export type RadioOptionType = {
  label: string;
  value: string;
  description?: string;
  disabled?: boolean;
  visibility?: boolean;
};

type Props = {
  idx: number;
  groupName: string;
  option: RadioOptionType;
  selected: string;
  disabled: boolean;
  onChange: (value: string) => void;
}

const RadioOption: FC<Props> = ({ idx, groupName, option, selected, disabled, onChange }) => {
  const id = `${groupName}-${idx}`;

  const { visibility = true } = option;

  if (!visibility) return null;

  return (
    <label
      key={id}
      htmlFor={id}
      className="vm-radio-group-option"
      aria-disabled={disabled || option.disabled}
    >
      <input
        className="vm-radio-group-option__input"
        id={id}
        type="radio"
        name={groupName}
        checked={selected === option.value}
        onChange={() => onChange(option.value)}
        disabled={disabled || option.disabled}
      />
      <span className="vm-radio-group-option__label">
        {option.label}
      </span>
      {option.description && (
      <span className="vm-radio-group-option__description">
        {option.description}
      </span>
        )}
    </label>
  );
};

export default RadioOption;
