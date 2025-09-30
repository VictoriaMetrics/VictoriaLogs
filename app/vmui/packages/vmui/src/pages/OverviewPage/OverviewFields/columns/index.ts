import { Column } from "../../../../components/Table/Table";
import { LogsFiledValues } from "../../../../api/types";
import { getFieldCol, getHitsCol, getPercentCol } from "./utils";

export const fieldNamesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Field name"),
  getHitsCol(),
  getPercentCol("Coverage %"),
];

export const fieldValuesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Field value"),
  getHitsCol(),
  getPercentCol("% of logs"),
];

export const streamFieldNamesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Stream field name"),
  getHitsCol(),
  getPercentCol("Coverage %"),
];

export const streamFieldValuesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Stream field value"),
  getHitsCol(),
  getPercentCol("% of logs"),
];
