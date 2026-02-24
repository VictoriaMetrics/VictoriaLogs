import { FC } from "preact/compat";
import { AutocompleteOptions } from "../Autocomplete";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import "./style.scss";

type Props = {
  option?: AutocompleteOptions
}

const AutocompleteDetailsPanel: FC<Props> = ({ option }) => {
  const { isMobile } = useDeviceDetect();
  const { description, type } = option || {};

  if (!description || isMobile) return null;

  return (
    <div className="vm-autocomplete-details-panel">
      <div className="vm-autocomplete-details-panel__type">{type}</div>
      <div
        className="vm-autocomplete-details-panel__description vm-markdown"
        dangerouslySetInnerHTML={{ __html: description || "" }}
      />
    </div>
  );
};

export default AutocompleteDetailsPanel;
