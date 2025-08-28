import { FC, ReactNode } from "preact/compat";
import "./style.scss";
import { MoreIcon } from "../../../../components/Main/Icons";
import Button from "../../../../components/Main/Button/Button";
import useBoolean from "../../../../hooks/useBoolean";
import { useRef } from "react";
import Popper from "../../../../components/Main/Popper/Popper";

type TopMenuItem = {
  icon: ReactNode;
  label: string;
  shortcut?: string;
  onClick: (e: MouseEvent) => void;
}

type TopMenu = TopMenuItem[];

type Props = {
  sections: TopMenu[]
}

const TopRowMenu: FC<Props> = ({ sections }) => {
  const {
    value: isOpenMenu,
    toggle: toggleOpenMenu,
    setFalse: handleCloseMenu,
  } = useBoolean(false);
  const menuButtonRef = useRef<HTMLDivElement>(null);

  const handleClickMenu = (e: MouseEvent) => {
    e.stopPropagation();
    toggleOpenMenu();
  };


  return (
    <>
      <div
        className="vm-top-row-menu-button"
        ref={menuButtonRef}
      >
        <Button
          size="small"
          color="gray"
          variant="text"
          startIcon={<MoreIcon/>}
          onClick={handleClickMenu}
        />
      </div>

      <Popper
        open={isOpenMenu}
        placement="bottom-right"
        onClose={handleCloseMenu}
        buttonRef={menuButtonRef}
      >
        <div className="vm-top-row-menu">
          {sections.map((menu, index) => (
            <div
              className="vm-top-row-menu-section"
              key={index}
            >
              {menu.map(({ icon, label, shortcut, onClick }: TopMenuItem) => (
                <div
                  className="vm-top-row-menu-item"
                  key={label}
                  onClick={(e) => {
                    handleCloseMenu();
                    onClick(e);
                  }}
                >
                  <div className="vm-top-row-menu-item__icon">{icon}</div>
                  <div className="vm-top-row-menu-item__label">{label}</div>
                  {shortcut && <div className="vm-top-row-menu-item__shortcut">{shortcut}</div>}
                </div>
              ))}
            </div>
          ))}
        </div>
      </Popper>
    </>
  );
};

export default TopRowMenu;
