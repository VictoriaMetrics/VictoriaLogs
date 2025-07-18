const router = {
  home: "/",
  icons: "/icons",
};

export interface RouterOptionsHeader {
  tenant?: boolean,
  stepControl?: boolean,
  timeSelector?: boolean,
  executionControls?: boolean,
  globalSettings?: boolean,
  cardinalityDatePicker?: boolean
}

export interface RouterOptions {
  title?: string,
  header: RouterOptionsHeader
}

export const routerOptions: { [key: string]: RouterOptions } = {
  [router.home]: {
    title: "Logs Explorer",
    header: {}
  },
  [router.icons]: {
    title: "Icons",
    header: {}
  },
};

export default router;
