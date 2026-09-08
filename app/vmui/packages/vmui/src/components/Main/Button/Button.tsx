import { ElementType, ComponentPropsWithoutRef, ReactNode } from "preact/compat";
import classNames from "classnames";
import "./style.scss";

type ButtonOwnProps<E extends ElementType = ElementType> = {
  as?: E;
  variant?: "contained" | "outlined" | "text";
  color?: "primary" | "secondary" | "success" | "error" | "gray" | "warning" | "white";
  size?: "small" | "medium" | "large";
  endIcon?: ReactNode;
  startIcon?: ReactNode;
  fullWidth?: boolean;
  children?: ReactNode;
  className?: string;
};

type ButtonProps<E extends ElementType> = ButtonOwnProps<E> &
  Omit<ComponentPropsWithoutRef<E>, keyof ButtonOwnProps<E>>;

const defaultElement = "button";

const Button = <E extends ElementType = typeof defaultElement>({
  as,
  variant = "contained",
  color = "primary",
  size = "medium",
  children,
  endIcon,
  startIcon,
  fullWidth = false,
  className,
  ...rest
}: ButtonProps<E>) => {
  const Tag = as ?? defaultElement;

  const classesButton = classNames(
    "vm-button",
    `vm-button_${variant}_${color}`,
    `vm-button_${size}`,
    {
      "vm-button_icon_only": (startIcon || endIcon) && !children,
      "vm-button_full-width": fullWidth,
      "vm-button_with-icons": startIcon || endIcon,
    },
    className,
  );

  return (
    <Tag
      className={classesButton}
      {...rest}
    >
      {startIcon}
      {children}
      {endIcon}
    </Tag>
  );
};

export default Button;
