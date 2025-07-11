export const getDefaultServer = (): string => {
  return window.location.href.replace(/(\/(select\/)?vmui\/.*|\/#\/.*)/, "");
};
