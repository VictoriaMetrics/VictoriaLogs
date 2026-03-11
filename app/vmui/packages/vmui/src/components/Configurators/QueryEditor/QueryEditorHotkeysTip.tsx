import { FC } from "preact/compat";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { ctrlKeyLabel } from "../../../utils/keyboard";
import { KeyboardIcon } from "../../Main/Icons";

const QueryEditorHotkeysTip: FC = () => {

  return (
    <Tooltip
      placement="bottom-right"
      title={
        <div className="vm-query-editor-help-tooltip">
          <p className="vm-query-editor-help-tooltip-item">
            <span>Shift + Enter</span> <span>insert a new line</span>
          </p>
          <p className="vm-query-editor-help-tooltip-item">
            <span>{ctrlKeyLabel} + Enter</span> <span>execute query</span>
          </p>
          <p className="vm-query-editor-help-tooltip-item">
            <span>{ctrlKeyLabel} + /</span> <span>toggle line comment</span>
          </p>
          <p className="vm-query-editor-help-tooltip-item">
            <span>{ctrlKeyLabel} + ↑</span> <span>previous query</span>
          </p>
          <p className="vm-query-editor-help-tooltip-item">
            <span>{ctrlKeyLabel} + ↓</span> <span>next query</span>
          </p>
        </div>
      }
    >
      <KeyboardIcon/>
    </Tooltip>
  );
};

export default QueryEditorHotkeysTip;
