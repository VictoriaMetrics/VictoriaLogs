import { FC, useRef } from "preact/compat";
import FieldCopyButton from "./FieldCopyButton";
import FieldMessageToggle from "./FieldMessageToggle";
import FieldGroupingToggle from "./FieldGroupingToggle";
import FieldFilterExclude from "./FieldFilterExclude";
import FieldFilterInclude from "./FieldFilterInclude";
import Button from "../../../Main/Button/Button";
import { MoreIcon } from "../../../Main/Icons";
import useBoolean from "../../../../hooks/useBoolean";
import Popper from "../../../Main/Popper/Popper";

type Props = {
  field: string;
  value: string;
  isStreamField: boolean;
  hideGroupButton: boolean;
}

const FieldRowActions: FC<Props> = ({ field, value, isStreamField, hideGroupButton }) => {
  const {
    value: openContextMenu,
    setFalse: handleCloseContextMenu,
    toggle: handleToggleContextMenu
  } = useBoolean(false);

  const buttonRef = useRef<HTMLDivElement>(null);

  const handleClick = (e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    handleToggleContextMenu();
  };

  return (
    <div className="vm-group-logs-row-fields-item-controls__wrapper">
      <FieldCopyButton
        field={field}
        value={value}
      />

      <FieldFilterInclude
        field={field}
        value={value}
        isStreamField={isStreamField}
      />

      <div ref={buttonRef}>
        <Button
          startIcon={<MoreIcon/>}
          variant="text"
          size="small"
          onClick={handleClick}
        />
      </div>

      <Popper
        placement="bottom-right"
        open={openContextMenu}
        buttonRef={buttonRef}
        onClose={handleCloseContextMenu}
      >
        <div className="vm-legend-hits-menu">
          <div className="vm-legend-hits-menu-section">
            <FieldFilterExclude
              field={field}
              value={value}
              isStreamField={isStreamField}
              onClose={handleCloseContextMenu}
            />
            <FieldMessageToggle
              field={field}
              onClose={handleCloseContextMenu}
            />
            {!hideGroupButton && (
              <FieldGroupingToggle
                field={field}
                onClose={handleCloseContextMenu}
              />
            )}
          </div>
        </div>
      </Popper>
    </div>
  );
};

export default FieldRowActions;
