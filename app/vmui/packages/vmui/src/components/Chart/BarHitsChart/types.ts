export enum GRAPH_STYLES {
  BAR = "Bars",
  LINE = "Lines",
  LINE_STEPPED = "Stepped lines",
  POINTS = "Points",
}

export enum GRAPH_QUERY_MODE {
  hits = "hits",
  stats = "stats"
}

export interface GraphOptions {
  graphStyle: GRAPH_STYLES;
  queryMode: GRAPH_QUERY_MODE;
  stacked: boolean;
  fill: boolean;
  hideChart: boolean;
}
