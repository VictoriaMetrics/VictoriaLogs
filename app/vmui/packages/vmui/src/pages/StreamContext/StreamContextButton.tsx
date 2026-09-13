import { FC } from "react";
import Tooltip from "../../components/Main/Tooltip/Tooltip";
import Button from "../../components/Main/Button/Button";
import { ContextIcon } from "../../components/Main/Icons";
import { Logs } from "../../api/types";
import useBoolean from "../../hooks/useBoolean";
import Modal from "../../components/Main/Modal/Modal";
import StreamContextList from "./StreamContextList";
import { LOGS_STREAM_CONTEXT_KEYS } from "../../constants/logs";

interface Props {
  log: Logs;
  displayFields?: string[];
}

const StreamContextButton: FC<Props> = ({ log, displayFields }) => {
  const showContextButton = LOGS_STREAM_CONTEXT_KEYS.every(field => log[field]);

  const {
    value: isOpenContext,
    setTrue: handleOpenContext,
    setFalse: handleCloseContext,
  } = useBoolean(false);

  const handleClickButton = (e: MouseEvent) => {
    e.stopPropagation();
    handleOpenContext();
  };

  const handleCloseModal = () => {
    handleCloseContext();
  };

  if (!showContextButton) {
    return null;  // Cannot show context without required anchor fields
  }

  return (
    <>
      <Tooltip title="Show context">
        <Button
          variant="text"
          color="gray"
          startIcon={<ContextIcon/>}
          onClick={handleClickButton}
          aria-label="show context"
        />
      </Tooltip>
      {isOpenContext && (
        <Modal
          title={"Log context (oldest to newest)"}
          isOpen={isOpenContext}
          onClose={handleCloseModal}
          className="vm-steam-context__modal"
        >
          <StreamContextList
            log={log}
            displayFields={displayFields}
          />
        </Modal>
      )}
    </>
  );
};

export default StreamContextButton;
