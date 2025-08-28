import { Column } from "../../../../components/Table/Table";
import { LogsFiledValues } from "../../../../api/types";
import { getFieldCol, getHitsCol, getPercentCol } from "./utils";

export const fieldNamesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Field name"),
  getHitsCol(),
  getPercentCol("Coverage %"),
  // getActionCol("Click by row for view field values")
];

export const fieldValuesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Field value"),
  getHitsCol(),
  getPercentCol("% of logs"),
  // getActionCol("Click by row for apply filter by field value")
];

export const streamFieldNamesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Stream field name"),
  getHitsCol(),
  getPercentCol("Coverage %"),
  // getActionCol("Click by row for view stream field values")
];

export const streamFieldValuesCol: Column<LogsFiledValues>[] = [
  getFieldCol("Stream field value"),
  getHitsCol(),
  getPercentCol("% of logs"),
  // getActionCol("Click by row for apply filter by stream field value")
];
