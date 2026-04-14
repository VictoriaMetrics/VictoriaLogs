import { ContextType } from "./types";
import { FunctionIcon } from "../../../Main/Icons";
import { pipes } from "../../../../generated/logsql.pipes";

export const pipeList = pipes.map(item => ({
  ...item,
  type: ContextType.PipeName,
  icon: <FunctionIcon/>,
}));
