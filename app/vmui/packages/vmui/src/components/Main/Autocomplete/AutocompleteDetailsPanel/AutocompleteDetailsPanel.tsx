import { FC } from "preact/compat";
import { AutocompleteOptions } from "../Autocomplete";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import "./style.scss";

type Props = {
  option?: AutocompleteOptions
  onPointerDownDetails?: () => void
}

const AutocompleteDetailsPanel: FC<Props> = ({ option, onPointerDownDetails }) => {
  const { isMobile } = useDeviceDetect();
  const { description, type } = option || {};

  if (!description || isMobile) return null;

  return (
    <div
      className="vm-autocomplete-details-panel"
      onPointerDown={onPointerDownDetails}
    >
      <div className="vm-autocomplete__title">{type}</div>
      <div
        className="vm-autocomplete-details-panel__description vm-markdown"
        dangerouslySetInnerHTML={{ __html: description || "" }}
      />
    </div>
  );
};

export default AutocompleteDetailsPanel;
