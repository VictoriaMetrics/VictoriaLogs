import { FC } from "preact/compat";
import Switch from "../../Main/Switch/Switch";
import { useLogsDispatch, useLogsState } from "../../../state/logsPanel/LogsStateContext";

const LogParsingSwitches: FC = () => {
  const { markdownParsing, ansiParsing } = useLogsState();
  const dispatch = useLogsDispatch();

  const handleChangeMarkdownParsing = (val: boolean) => {
    dispatch({ type: "SET_MARKDOWN_PARSING", payload: val });

    if (ansiParsing) {
      dispatch({ type: "SET_ANSI_PARSING", payload: false });
    }
  };

  const handleChangeAnsiParsing = (val: boolean) => {
    dispatch({ type: "SET_ANSI_PARSING", payload: val });

    if (markdownParsing) {
      dispatch({ type: "SET_MARKDOWN_PARSING", payload: false });
    }
  };

  return (
    <>
      <div className="vm-group-logs-configurator-item">
        <Switch
          label={"Markdown parsing"}
          value={markdownParsing}
          onChange={handleChangeMarkdownParsing}
        />
        <div className="vm-group-logs-configurator-item__info">
          Parses log text and renders Markdown formatting.
        </div>
      </div>
      <div className="vm-group-logs-configurator-item">
        <Switch
          label={"ANSI parsing"}
          value={ansiParsing}
          onChange={handleChangeAnsiParsing}
        />
        <div className="vm-group-logs-configurator-item__info">
          Renders ANSI escape codes as colored text.
        </div>
      </div>
    </>
  );
};

export default LogParsingSwitches;
