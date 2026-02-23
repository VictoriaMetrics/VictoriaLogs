import {
  FC,
  ReactNode,
  useEffect,
  useMemo,
  useRef,
  useState,
  useCallback,
  createPortal,
  RefObject
} from "preact/compat";
import classNames from "classnames";
import "./style.scss";
import useClickOutside from "../../../hooks/useClickOutside";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import Button from "../Button/Button";
import { CloseIcon } from "../Icons";
import { useLocation, useNavigate } from "react-router-dom";
import useEventListener from "../../../hooks/useEventListener";
import { Size, useResizeObserver } from "../../../hooks/useResizeObserver";

interface PopperProps {
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

const Popper: FC<PopperProps> = ({
  children,
  buttonRef,
  placement = "bottom-left",
  placementPosition,
  open = false,
  onClose,
  offset = { top: 6, left: 0 },
  clickOutside = true,
  fullWidth,
  title,
  disabledFullScreen,
  variant
}) => {
  const { isMobile } = useDeviceDetect();
  const navigate = useNavigate();
  const location = useLocation();
  const [popperSize, setPopperSize] = useState<Size>({ width: 0, height: 0 });
  const [isOpen, setIsOpen] = useState(false);

  const popperRef = useRef<HTMLDivElement>(null);

  const popperStyle = useMemo(() => {
    const buttonEl = buttonRef.current;

    if (!buttonEl || !open) return {};

    const buttonPos = buttonEl.getBoundingClientRect();

    const position = {
      top: 0,
      left: 0,
      width: "auto",
    };

    const needAlignRight = placement === "bottom-right" || placement === "top-right";
    const needAlignTop = placement === "top-left" || placement === "top-right";

    const popperWidth = popperSize.width || 0;
    const popperHeight = popperSize.height || 0;

    const offsetTop = offset.top || 0;
    const offsetLeft = offset.left || 0;

    const { innerWidth, innerHeight } = window;

    const maxLeft = Math.max(innerWidth - popperWidth, 0);
    const maxTop = Math.max(innerHeight - popperHeight, 0);

    position.top = buttonPos.top + buttonPos.height + offsetTop;
    position.left = buttonPos.left + offsetLeft;

    if (needAlignTop) position.top = buttonPos.top - popperHeight - offsetTop;
    if (needAlignRight) position.left = buttonPos.right - popperWidth - offsetLeft;
    if (fullWidth) position.width = `${buttonPos.width}px`;

    if (placement === "fixed" && placementPosition) {
      const clampedTop = Math.max(placementPosition.top + offsetTop, 0);
      const clampedLeft = Math.max(placementPosition.left + offsetLeft, 0);

      position.top = Math.min(clampedTop, maxTop);
      position.left = Math.min(clampedLeft, maxLeft);

      return position;
    }

    const isOverflowBottom = position.top + popperHeight > innerHeight;
    const isOverflowTop = position.top < 0;
    const isOverflowRight = position.left + popperWidth > innerWidth;
    const isOverflowLeft = position.left < 0;

    if (isOverflowBottom) position.top = buttonPos.top - popperHeight - offsetTop;
    if (isOverflowTop) position.top = buttonPos.top + buttonPos.height + offsetTop;
    if (isOverflowRight) position.left = maxLeft;
    if (isOverflowLeft) position.left = buttonPos.left + offsetLeft;

    position.top = Math.min(Math.max(position.top, 0), maxTop);
    position.left = Math.min(Math.max(position.left, 0), maxLeft);

    return position;
  }, [
    buttonRef,
    open,
    placement,
    placementPosition,
    offset.top,
    offset.left,
    fullWidth,
    popperSize.width,
    popperSize.height,
  ]);

  const handleClickClose = (e: MouseEvent) => {
    e.stopPropagation();
    onClose();
  };

  const handleClose = () => {
    setIsOpen(false);
    onClose();
  };

  const handleClickOutside = () => {
    if (!clickOutside) return;
    handleClose();
  };

  const handlePopstate = useCallback(() => {
    if (isOpen && isMobile && !disabledFullScreen) {
      navigate(location, { replace: true });
      onClose();
    }
  }, [isOpen, isMobile, disabledFullScreen, location, onClose]);

  useEventListener("scroll", handleClose);
  useEventListener("popstate", handlePopstate);
  useResizeObserver({ ref: popperRef, onResize: setPopperSize });
  useClickOutside(popperRef, handleClickOutside, buttonRef);

  useEffect(() => {
    setIsOpen(open);

    if (!open && onClose) onClose();
    if (open && isMobile && !disabledFullScreen) {
      document.body.style.overflow = "hidden";
    }

    return () => {
      document.body.style.overflow = "auto";
    };
  }, [open]);

  useEffect(() => {
    setIsOpen(false);
  }, [popperRef]);

  return (
    <>
      {(isOpen || !popperSize.width) && createPortal((
        <div
          className={classNames({
            "vm-popper": true,
            [`vm-popper_${variant}`]: variant,
            "vm-popper_mobile": isMobile && !disabledFullScreen,
            "vm-popper_open": (isMobile || Object.keys(popperStyle).length) && isOpen,
          })}
          ref={popperRef}
          style={(isMobile && !disabledFullScreen) ? {} : popperStyle}
        >
          {(title || (isMobile && !disabledFullScreen)) && (
            <div className="vm-popper-header">
              <p className="vm-popper-header__title">{title}</p>
              <Button
                variant="text"
                color={variant === "dark" ? "white" : "primary"}
                size="small"
                onClick={handleClickClose}
                ariaLabel="close"
              >
                <CloseIcon/>
              </Button>
            </div>
          )}
          {children}
        </div>), document.body)}
    </>
  );
};

export default Popper;
