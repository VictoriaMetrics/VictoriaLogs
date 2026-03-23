import { FC } from "preact/compat";
import Button from "../../../Main/Button/Button";
import { ListAllIcon } from "../../../Main/Icons";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import useBoolean from "../../../../hooks/useBoolean";
import Modal from "../../../Main/Modal/Modal";
import QueryExamplesModal from "./QueryExamplesModal";

type Props = {
  onApply: (value: string) => void;
}

const QueryExamplesButton: FC<Props> = ({ onApply }) => {
  const { isMobile } = useDeviceDetect();

  const {
    value: isOpenExamples,
    setTrue: openExamples,
    setFalse: closeExamples,
  } = useBoolean(false);

  const handleApplyQuery = (value: string) => {
    onApply(value);
    closeExamples();
  };

  return (
    <>
      <Button
        color="gray"
        variant="outlined"
        onClick={openExamples}
        startIcon={<ListAllIcon/>}
      >
        {isMobile ? "" : "Query examples"}
      </Button>

      {isOpenExamples && (
        <Modal
          title={"Query examples"}
          isOpen={isOpenExamples}
          onClose={closeExamples}
        >
          <QueryExamplesModal onApply={handleApplyQuery}/>
        </Modal>
      )}
    </>
  );
};

export default QueryExamplesButton;
