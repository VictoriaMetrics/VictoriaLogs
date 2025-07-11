import { Logs } from "../../../api/types";
import { RefObject } from "preact/compat";

export interface ViewProps {
  data: Logs[];
  settingsRef: RefObject<HTMLDivElement>;
}
