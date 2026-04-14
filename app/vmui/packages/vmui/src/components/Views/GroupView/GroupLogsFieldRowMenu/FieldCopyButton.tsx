import { FC, useEffect, useState } from "preact/compat";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { useCallback } from "react";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import Button from "../../../Main/Button/Button";
import { CopyIcon, DoneIcon } from "../../../Main/Icons";

type Props = {
  field: string;
  value: string;
}

const FieldCopyButton: FC<Props> = ({ field, value }) => {
  const copyToClipboard = useCopyToClipboard();

  const [copied, setCopied] = useState<boolean>(false);

  const handleCopy = useCallback(async () => {
    if (copied) return;
    try {
      await copyToClipboard(`${field}: ${JSON.stringify(value)}`);
      setCopied(true);
    } catch (e) {
      console.error(e);
    }
  }, [field, value, copied, copyToClipboard]);

  useEffect(() => {
    if (copied === null) return;
    const timeout = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(timeout);
  }, [copied]);

  return (
    <Tooltip title={copied ? "Copied" : "Copy field:value pair"}>
      <Button
        variant="text"
        color="gray"
        size="small"
        startIcon={copied ? <DoneIcon/> : <CopyIcon/>}
        onClick={handleCopy}
      />
    </Tooltip>
  );
};

export default FieldCopyButton;
