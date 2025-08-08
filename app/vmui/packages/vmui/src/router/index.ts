const router = {
  home: "/",
  streamContext: "/stream-context/:_stream_id/:_time",
  icons: "/icons",
};

export interface RouterOptionsHeader {
  tenant?: boolean,
  timeSelector?: boolean,
  executionControls?: boolean,
}

export interface RouterOptions {
  title?: string,
  header: RouterOptionsHeader
}

export const routerOptions: { [key: string]: RouterOptions } = {
  [router.home]: {
    title: "Logs Explorer",
    header: {
      tenant: true,
      timeSelector: true,
      executionControls: true,
    }
  },
  [router.icons]: {
    title: "Icons",
    header: {}
  },
  [router.streamContext]: {
    title: "Stream context",
    header: {}
  }
};

export default router;
