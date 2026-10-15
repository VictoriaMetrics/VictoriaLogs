import { Logs } from "../../../api/types";
import { TimeParams } from "../../../types";
import { RefObject } from "preact/compat";

export interface ViewProps {
  data: Logs[];
  settingsRef: RefObject<HTMLDivElement>;
  period?: TimeParams;
}
