import Button from "../../../../components/Main/Button/Button";
import { QuestionIcon } from "../../../../components/Main/Icons";
import "./style.scss";
import useBoolean from "../../../../hooks/useBoolean";
import Modal from "../../../../components/Main/Modal/Modal";
import OverviewFieldsHelpContent from "./OverviewFieldsHelpContent";

const OverviewFieldsHelps = () => {
  const {
    value: openModal,
    setTrue: handleOpenModal,
    setFalse: handleCloseModal,
  } = useBoolean(false);

  return (
    <>
      <Button
        startIcon={<QuestionIcon/>}
        variant="text"
        onClick={handleOpenModal}
      >
        How it works
      </Button>
      {openModal && (
        <Modal
          title={"Fields & Streams Overview"}
          onClose={handleCloseModal}
        >
          <div className="vm-overview-fields-tour">
            <OverviewFieldsHelpContent/>
          </div>
        </Modal>
      )}
    </>
  );
};

export default OverviewFieldsHelps;
