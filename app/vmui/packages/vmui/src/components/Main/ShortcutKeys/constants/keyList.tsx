import { isMacOs } from "../../../../utils/detect-device";
import { altKeyLabel, ctrlKeyLabel } from "../../../../utils/keyboard";

const ctrlMeta = <code>{ctrlKeyLabel}</code>;

export const AUTOCOMPLETE_QUICK_KEY = <>{<code>{isMacOs() ? altKeyLabel : "Ctrl"}</code>} + <code>Space</code></>;

const keyList = [
  {
    title: "Query",
    list: [
      {
        keys: <code>Enter</code>,
        description: "Run"
      },
      {
        keys: <><code>Shift</code> + <code>Enter</code></>,
        description: "Multi-line queries"
      },
      {
        keys: <>{ctrlMeta} + <code>Arrow Up</code></>,
        description: "Previous command from the Query history"
      },
      {
        keys: <>{ctrlMeta} + <code>Arrow Down</code></>,
        description: "Next command from the Query history"
      },
      {
        keys: AUTOCOMPLETE_QUICK_KEY,
        description: "Show quick autocomplete tips"
      }
    ]
  },
  {
    title: "Graph",
    list: [
      {
        keys: <>{ctrlMeta} + <code>scroll Up</code> or <code>+</code></>,
        description: "Zoom in"
      },
      {
        keys: <>{ctrlMeta} + <code>scroll Down</code> or <code>-</code></>,
        description: "Zoom out"
      },
      {
        keys: <>{ctrlMeta} + <code>drag</code></>,
        description: "Move the graph left/right"
      },
      {
        keys: <><code>click</code> on legend item</>,
        description: "Open the legend item menu"
      },
      {
        keys: <><code>{altKeyLabel}</code> + <code>click</code> on legend item</>,
        description: "Hide or show this series"
      },
      {
        keys: <>{ctrlMeta} + <code>click</code> on legend item</>,
        description: "Show only this series or show all series"
      }
    ]
  },
];

export default keyList;
