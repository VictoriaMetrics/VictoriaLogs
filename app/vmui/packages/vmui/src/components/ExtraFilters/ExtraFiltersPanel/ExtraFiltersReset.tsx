import { FC } from "preact/compat";
import Button from "../../Main/Button/Button";
import { useExtraFilters } from "../hooks/useExtraFilters";
import { RestartIcon } from "../../Main/Icons";

const ExtraFiltersToQueryButton: FC = () => {
  const { extraFilters, clearFilters } = useExtraFilters();

  if (!extraFilters.length) return null;

  return (
    <Button
      color="gray"
      variant="outlined"
      startIcon={<RestartIcon/>}
      onClick={clearFilters}
    >
      Clear filters
    </Button>
  );
};

export default ExtraFiltersToQueryButton;
