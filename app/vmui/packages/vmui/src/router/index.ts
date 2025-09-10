const router = {
  home: "/",
  streamContext: "/stream-context/:_stream_id/:_time",
  icons: "/icons",
  rules: "/rules",
  notifiers: "/notifiers"
};

export interface RouterOptionsHeader {
  tenant?: boolean;
  stepControl?: boolean;
  timeSelector?: boolean;
  executionControls?: ExecutionControlsProps;
  globalSettings?: boolean;
  cardinalityDatePicker?: boolean;
}

export interface RouterOptions {
  title?: string,
  header: RouterOptionsHeader
}

interface ExecutionControlsProps {
  tooltip: string;
  useAutorefresh: boolean;
}

export const routerOptions: { [key: string]: RouterOptions } = {
  [router.home]: {
    title: "Logs Explorer",
    header: {
      tenant: true,
      timeSelector: true,
      executionControls: {
        tooltip: "Refresh dashboard",
        useAutorefresh: true,
      }
    }
  },
  [router.icons]: {
    title: "Icons",
    header: {}
  },
  [router.streamContext]: {
    title: "Stream context",
    header: {}
  },
  [router.rules]: {
    title: "Rules",
    header: {
      executionControls: {
        tooltip: "Refresh alerts",
        useAutorefresh: false,
      }
    },
  },
  [router.notifiers]: {
    title: "Notifiers",
    header: {
      executionControls: {
        tooltip: "Refresh notifiers",
        useAutorefresh: false,
      },
    },
  },
};

export default router;
