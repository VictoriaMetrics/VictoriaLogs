import uPlot from "uplot";
import { ReactNode } from "preact/compat";

export interface MetricBase {
  group: number;
  metric: {
    [key: string]: string;
  };
}

export interface MetricResult extends MetricBase {
  values: [number, string][];
}


export interface InstantMetricResult extends MetricBase {
  value?: [number, string];
  values?: [number, string][];
}

export interface QueryStats {
  seriesFetched?: string;
  executionTimeMsec?: number;
  resultLength?: number;
  isPartial?: boolean;
}

export interface Logs {
  _msg: string;
  _stream: string;
  _time: string;
  [key: string]: string;
}

export interface LogHits {
  timestamps: string[];
  values: number[];
  total: number;
  fields: { [key: string]: string; };
  _isOther: boolean;
}

export interface LegendLogHits {
  label: string;
  total: number;
  totalHits: number;
  isOther: boolean;
  fields: { [key: string]: string; };
  stroke?: uPlot.Series.Stroke;
}

export interface LegendLogHitsMenu {
  title: string;
  icon?: ReactNode;
  handler?: () => void;
}

export interface LogsFiledValues {
  value: string;
  hits: number;
}
