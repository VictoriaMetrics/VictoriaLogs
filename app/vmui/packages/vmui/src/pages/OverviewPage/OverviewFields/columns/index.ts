import { type Column } from "../../../../components/Table/types";
import { LogsFieldValues } from "../../../../api/types";
import { getFieldCol, getHitsCol, getPercentCol } from "./utils";

export const fieldNamesCol: Column<LogsFieldValues>[] = [
  getFieldCol("Field name"),
  getHitsCol(),
  getPercentCol("Coverage %"),
];

export const fieldValuesCol: Column<LogsFieldValues>[] = [
  getFieldCol("Field value"),
  getHitsCol(),
  getPercentCol("% of logs"),
];

export const streamFieldNamesCol: Column<LogsFieldValues>[] = [
  getFieldCol("Stream field name"),
  getHitsCol(),
  getPercentCol("Coverage %"),
];

export const streamFieldValuesCol: Column<LogsFieldValues>[] = [
  getFieldCol("Stream field value"),
  getHitsCol(),
  getPercentCol("% of logs"),
];
