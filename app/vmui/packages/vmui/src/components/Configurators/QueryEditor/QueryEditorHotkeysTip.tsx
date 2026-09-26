import { FC, Fragment } from "preact/compat";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { ctrlKeyLabel } from "../../../utils/keyboard";
import { KeyboardIcon } from "../../Main/Icons";
import { AUTOCOMPLETE_QUICK_KEY } from "../../Main/ShortcutKeys/constants/keyList";

const helpItems = [
  { keys: ["Shift", "Enter"], description: "insert a new line" },
  { keys: [ctrlKeyLabel, "Enter"], description: "execute query" },
  { keys: [ctrlKeyLabel, "/"], description: "toggle line comment" },
  { keys: [ctrlKeyLabel, "↑"], description: "previous query" },
  { keys: [ctrlKeyLabel, "↓"], description: "next query" },
  { keys: [AUTOCOMPLETE_QUICK_KEY], description: "show suggestions" },
];

const QueryEditorHotkeysTip: FC = () => {

  return (
    <Tooltip
      placement="bottom-right"
      title={
        <div className="vm-query-editor-help-tooltip">
          {helpItems.map(({ keys, description }) => (
            <p
              key={description}
              className="vm-query-editor-help-tooltip-item"
            >
              <span>
                {keys.map((key, index) => (
                  <Fragment key={`${key}-${index}`}>
                    {index > 0 && " + "}
                    <code>{key}</code>
                  </Fragment>
          ))}
              </span>
              <span>{description}</span>
            </p>
          ))}
        </div>
      }
    >
      <KeyboardIcon/>
    </Tooltip>
  );
};

export default QueryEditorHotkeysTip;
