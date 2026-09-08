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

export const scrollElementToCenter = (
  scrollContainer: HTMLElement,
  targetElement: HTMLElement,
) => {
  const containerRect = scrollContainer.getBoundingClientRect();
  const targetRect = targetElement.getBoundingClientRect();

  const targetOffset = targetRect.top - containerRect.top + scrollContainer.scrollTop;

  scrollContainer.scrollTop = targetOffset - scrollContainer.clientHeight / 2 + targetElement.clientHeight / 2;
};
