import { FC, useEffect, useState } from "preact/compat";
import useCopyToClipboard from "../../../hooks/useCopyToClipboard";
import Button from "../../Main/Button/Button";
import { CopyIcon, DoneIcon } from "../../Main/Icons";

type Props = {
  getData: () => string;
}

const TableCopyButton: FC<Props> = ({ getData }) => {
  const copyToClipboard = useCopyToClipboard();

  const [copied, setCopied] = useState(false);
  const handleCopyLog = async () => {
    try {
      const data = getData();
      const isCopied = await copyToClipboard(data, "Log copied to clipboard");
      if (isCopied) setCopied(true);
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    if (copied) {
      const timeout = setTimeout(() => setCopied(false), 2000);
      return () => clearTimeout(timeout);
    }
  }, [copied]);

  return (
    <Button
      startIcon={copied ? <DoneIcon/> : <CopyIcon/>}
      variant="text"
      color="gray"
      size="small"
      onClick={handleCopyLog}
      aria-label="Copy log"
    />
  );
};

export default TableCopyButton;
