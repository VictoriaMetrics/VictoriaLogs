const router = {
  home: "/",
  overview: "/overview",
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
    title: "Query",
    header: {
      tenant: true,
      timeSelector: true,
      executionControls: true,
    }
  },
  [router.overview]: {
    title: "Overview",
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
};

export default router;
