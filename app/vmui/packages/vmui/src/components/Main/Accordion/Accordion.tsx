import { FC, useState, useEffect, ReactNode, useId } from "preact/compat";
import { ArrowDownIcon } from "../Icons";
import "./style.scss";

interface AccordionProps {
  title: ReactNode
  children: ReactNode
  defaultExpanded?: boolean
  onChange?: (value: boolean) => void
}

const Accordion: FC<AccordionProps> = ({
  defaultExpanded = false,
  onChange,
  title,
  children
}) => {
  const [isOpen, setIsOpen] = useState(defaultExpanded);
  const titleId = useId();

  const toggleOpen = () => {
    const selection = window.getSelection();
    if (selection && selection.toString()) {
      return; // If the text is selected, cancel the execution of toggle.
    }

    setIsOpen((prev) => {
      const newState = !prev;
      onChange && onChange(newState);
      return newState;
    });
  };

  useEffect(() => {
    setIsOpen(defaultExpanded);
  }, [defaultExpanded]);

  return (
    <>
      <header
        className={`vm-accordion-header ${isOpen && "vm-accordion-header_open"}`}
        onClick={toggleOpen}
        id={titleId}
      >
        {title}
        <button
          type="button"
          className={`vm-accordion-header__arrow ${isOpen ? "vm-accordion-header__arrow_open" : ""}`}
          aria-expanded={isOpen}
          aria-labelledby={titleId}
        >
          <span aria-hidden="true"><ArrowDownIcon /></span>
        </button>
      </header>
      {isOpen && (
        <section
          className="vm-accordion-section"
          key="content"
        >
          {children}
        </section>
      )}
    </>
  );
};

export default Accordion;
