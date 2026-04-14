import {
  FC,
  ReactNode,
  RefObject
} from "preact/compat";
import "./style.scss";
import PopperContent from "./PopperContent";

export interface PopperProps {
  children: ReactNode
  open: boolean
  onClose: () => void
  buttonRef: RefObject<HTMLElement>
  placement?: "bottom-right" | "bottom-left" | "top-left" | "top-right" | "fixed"
  placementPosition?: { top: number, left: number } | null
  offset?: { top: number, left: number }
  clickOutside?: boolean,
  fullWidth?: boolean
  title?: string
  disabledFullScreen?: boolean
  variant?: "default" | "dark"
}

const Popper: FC<PopperProps> = (props) => {
  if (!props.open) return null;

  return (
    <PopperContent {...props}/>
  );
};

export default Popper;
