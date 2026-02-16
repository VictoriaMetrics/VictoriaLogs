export const borderBoxToContentSize = (
  el: HTMLElement,
  borderBoxSize: number,
  axis: "x" | "y"
): number => {
  const cs = getComputedStyle(el);
  if (cs.boxSizing === "content-box") return borderBoxSize;

  const sub = axis === "y"
    // y-axis
    ? parseFloat(cs.paddingTop) +
      parseFloat(cs.paddingBottom) +
      parseFloat(cs.borderTopWidth) +
      parseFloat(cs.borderBottomWidth)
    // x-axis
    : parseFloat(cs.paddingLeft) +
      parseFloat(cs.paddingRight) +
      parseFloat(cs.borderLeftWidth) +
      parseFloat(cs.borderRightWidth);

  return Math.max(0, borderBoxSize - sub);
};
