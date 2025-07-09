import { FC, useCallback } from "preact/compat";
import Button from "../Main/Button/Button";
import { CopyIcon } from "../Main/Icons";
import Tooltip from "../Main/Tooltip/Tooltip";
import useCopyToClipboard from "../../hooks/useCopyToClipboard";

interface Props {
  title: string;
  getData: () => string;
  successfulCopiedMessage: string;
}

export const CopyButton: FC<Props> = ({ title, getData, successfulCopiedMessage }) => {
  const copyToClipboard = useCopyToClipboard();
  const handleClick = useCallback(() => {
    copyToClipboard(getData(), successfulCopiedMessage);
  }, [getData, successfulCopiedMessage]);

  return <Tooltip
    title={title}
  >
    <Button
      variant="text"
      startIcon={<CopyIcon/>}
      onClick={handleClick}
      ariaLabel={title}
    />
  </Tooltip>;
};
