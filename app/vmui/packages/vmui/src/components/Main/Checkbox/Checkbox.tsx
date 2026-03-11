import classNames from "classnames";
import "./style.scss";
import { FC, ReactNode } from "preact/compat";
import { DoneIcon } from "../Icons";

interface CheckboxProps {
  checked: boolean
  color?: "primary" | "secondary" | "gray" | "inherit"
  disabled?: boolean
  label?: string
  size?: "small" | "medium" | "large"
  icon?: ReactNode;
  onChange?: (value: boolean) => void
}

const Checkbox: FC<CheckboxProps> = ({
  checked = false,
  disabled = false,
  label,
  color = "secondary",
  size = "medium",
  icon: customIcon,
  onChange
}) => {
  const toggleCheckbox = () => {
    if (disabled || !onChange) return;
    onChange(!checked);
  };

  const checkboxClasses = classNames({
    "vm-checkbox": true,
    "vm-checkbox_disabled": disabled,
    "vm-checkbox_active": checked,
    [`vm-checkbox_${color}_active`]: checked,
    [`vm-checkbox_${color}`]: color,
    [`vm-checkbox_${size}`]: size
  });

  return (
    <div
      className={checkboxClasses}
      onClick={toggleCheckbox}
    >
      <div className="vm-checkbox-track">
        <div className="vm-checkbox-track__thumb">
          {customIcon || <DoneIcon/>}
        </div>
      </div>
      {label && <span className="vm-checkbox__label">{label}</span>}
    </div>
  );
};

export default Checkbox;
