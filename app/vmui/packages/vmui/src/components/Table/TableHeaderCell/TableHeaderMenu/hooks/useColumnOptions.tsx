import {
  CopyIcon,
  UnwrapTextIcon,
  VisibilityOffIcon,
  WidthIcon,
  WrapTextIcon
} from "../../../../Main/Icons";
import { TableHeaderMenuProps } from "../TableHeaderMenu";
import { ReactNode } from "preact/compat";
import useCopyToClipboard from "../../../../../hooks/useCopyToClipboard";

type MenuOption = {
  title: string;
  icon: ReactNode;
  show?: boolean;
  onClick?: () => void;
}

export const useColumnOptions = <T extends object>({
  column,
  columnPrefs,
  onResize,
  onWrap,
  onHideCol,
}: TableHeaderMenuProps<T>) => {
  const copyToClipboard = useCopyToClipboard();
  const handleCopy = async (copyValue: string) => {
    await copyToClipboard(copyValue, `Field name \`${copyValue}\` has been copied`);
  };

  const { width, wrapped } = columnPrefs || {};

  const displayOptions: MenuOption[] = [
    {
      title: "Hide column",
      icon: <VisibilityOffIcon/>,
      onClick: onHideCol
    },
  ];

  const textOptions: MenuOption[] = [
    {
      title: wrapped ? "Unwrap text" : "Wrap text",
      icon: wrapped ? <UnwrapTextIcon/> : <WrapTextIcon/>,
      onClick: () => onWrap(!wrapped)
    },
    {
      title: "Copy field",
      icon: <CopyIcon/>,
      onClick: () => handleCopy(column.key)
    },
  ];

  const sizeOptions: MenuOption[] = [
    {
      title: "Auto width",
      icon: <WidthIcon/>,
      show: Boolean(width),
      onClick: () => onResize(0)
    },
  ];

  const sections = [
    displayOptions,
    textOptions,
    sizeOptions
  ].reduce(collectVisibleSections, []);

  return {
    sections
  };
};

const collectVisibleSections = (acc: MenuOption[][], section: MenuOption[]) => {
  const visible = section.filter(o => o.show !== false);
  if (visible.length) acc.push(visible);
  return acc;
};
