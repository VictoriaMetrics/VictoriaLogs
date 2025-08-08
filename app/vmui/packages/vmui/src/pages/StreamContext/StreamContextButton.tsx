import { FC, MouseEvent } from "react";
import Tooltip from "../../components/Main/Tooltip/Tooltip";
import Button from "../../components/Main/Button/Button";
import { ContextIcon } from "../../components/Main/Icons";
import { Logs } from "../../api/types";
import useBoolean from "../../hooks/useBoolean";
import Modal from "../../components/Main/Modal/Modal";
import { useMemo } from "preact/compat";
import StreamContextList from "./StreamContextList";

interface Props {
  log: Logs;
  displayFields?: string[];
}

const StreamContextButton: FC<Props> = ({ log, displayFields }) => {
  const requiredFields = ["_stream_id", "_time"];
  const showContextButton = useMemo(() => requiredFields.every(field => log[field]), [log]);

  const {
    value: isOpenContext,
    setTrue: handleOpenContext,
    setFalse: handleCloseContext,
  } = useBoolean(false);

  const handleClickButton = (e: MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation();
    handleOpenContext();
  };

  const handleCloseModal = () => {
    handleCloseContext();
  };

  if (!showContextButton) {
    return null; // Cannot show context without stream ID
  }

  return (
    <>
      <Tooltip title="Show context">
        <Button
          variant="text"
          color="gray"
          startIcon={<ContextIcon/>}
          onClick={handleClickButton}
          ariaLabel="show context"
        />
      </Tooltip>
      {isOpenContext && (
        <Modal
          title={"Log context"}
          isOpen={isOpenContext}
          onClose={handleCloseModal}
        >
          <StreamContextList
            isModal
            log={log}
            displayFields={displayFields}
          />
        </Modal>
      )}
    </>
  );
};

export default StreamContextButton;
