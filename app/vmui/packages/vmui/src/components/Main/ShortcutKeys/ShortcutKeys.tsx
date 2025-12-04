import { FC, ReactNode, useCallback } from "preact/compat";
import Modal from "../Modal/Modal";
import "./style.scss";
import Tooltip from "../Tooltip/Tooltip";
import keyList from "./constants/keyList";
import { isMacOs } from "../../../utils/detect-device";
import useBoolean from "../../../hooks/useBoolean";
import useEventListener from "../../../hooks/useEventListener";
import { ctrlKeyLabel } from "../../../utils/keyboard";

const title = "Shortcut keys";
const isMac = isMacOs();
const keyOpenHelp = isMac ? `${ctrlKeyLabel} + /` : "F1";

type Props = {
  children?: ReactNode
}

const ShortcutKeys: FC<Props> = ({ children }) => {

  const {
    value: openList,
    setTrue: handleOpen,
    setFalse: handleClose,
  } = useBoolean(false);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    const openOnMac = isMac && e.key === "/" && e.metaKey;
    const openOnOther = !isMac && e.key === "F1" && !e.metaKey;
    if (openOnMac || openOnOther) {
      handleOpen();
    }
  }, [handleOpen]);

  useEventListener("keydown", handleKeyDown);

  return <>
    <Tooltip
      title={`${title} (${keyOpenHelp})`}
      placement="bottom-center"
    >
      <div onClick={handleOpen}>
        {children}
      </div>
    </Tooltip>

    {openList && (
      <Modal
        title={"Shortcut keys"}
        onClose={handleClose}
      >
        <div className="vm-shortcuts">
          {keyList.map(section => (
            <div
              className="vm-shortcuts-section"
              key={section.title}
            >
              <h3 className="vm-shortcuts-section__title">
                {section.title}
              </h3>
              <div className="vm-shortcuts-section-list">
                {section.list.map((l, i) => (
                  <div
                    className="vm-shortcuts-section-list-item"
                    key={`${section.title}_${i}`}
                  >
                    <div className="vm-shortcuts-section-list-item__key">
                      {l.keys}
                    </div>
                    <p className="vm-shortcuts-section-list-item__description">
                      {l.description}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </Modal>
    )}
  </>;
};

export default ShortcutKeys;
