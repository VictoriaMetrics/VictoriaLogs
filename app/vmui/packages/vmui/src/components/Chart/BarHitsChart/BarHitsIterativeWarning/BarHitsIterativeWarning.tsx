import { FC } from "preact/compat";
import Alert from "../../../Main/Alert/Alert";
import Button from "../../../Main/Button/Button";
import "./style.scss";
import { useQueryDispatch } from "../../../../state/query/QueryStateContext";

const BarHitsIterativeWarning: FC = () => {
  const dispatch = useQueryDispatch();

  const handleChangeIterative = () => {
    dispatch({ type: "EXECUTE_HITS_ONCE" });
  };

  return (
    <div className="vm-bar-hits-chart-iterative-warning">
      <Alert variant="warning">
        <div className="vm-bar-hits-chart-iterative-warning__content">
          <p>The chart was loaded incrementally. The legend is hidden because the top series may be inaccurate.</p>
          <Button
            color="warning"
            onClick={handleChangeIterative}
          >
            Load all at once
          </Button>
        </div>
      </Alert>
    </div>
  );
};

export default BarHitsIterativeWarning;
