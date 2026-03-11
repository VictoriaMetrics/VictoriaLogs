import { FC, useEffect, useState } from "preact/compat";
import Button from "../../Main/Button/Button";
import { useExtraFilters } from "../hooks/useExtraFilters";
import { CopyIcon, DoneIcon } from "../../Main/Icons";
import useCopyToClipboard from "../../../hooks/useCopyToClipboard";

const ExtraFiltersCopy: FC = () => {
  const copyToClipboard = useCopyToClipboard();

  const { extraParams } = useExtraFilters();
  const [isCopied, setCopied] = useState(false);

  const handleCopyFilters = async () => {
    const filters = Array.from(extraParams.values()).join("\n");
    const value = await copyToClipboard(filters, "Filters has been copied");
    setCopied(value);
  };

  useEffect(() => {
    if (!isCopied) return;
    const timeout = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(timeout);
  }, [isCopied]);

  if (!extraParams.size) return null;

  return (
    <Button
      color="gray"
      variant="outlined"
      onClick={handleCopyFilters}
      startIcon={isCopied ? <DoneIcon /> : <CopyIcon />}
    >
      Copy filters
    </Button>
  );
};

export default ExtraFiltersCopy;
