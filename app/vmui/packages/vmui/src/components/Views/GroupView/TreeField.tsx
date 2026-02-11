import { FC, useState, useCallback, useMemo } from "preact/compat";
import "./style.scss";
import classNames from "classnames";

interface TreeFieldProps {
  fieldKey: string;
  value: unknown;
  depth?: number;
  defaultExpanded?: boolean;
  path?: string[];
  isExpanded?: (path: string) => boolean;
  onToggle?: (path: string) => void;
}

const MAX_TREE_DEPTH = 50;

const parseJsonString = (str: string): unknown => {
  try {
    const parsed = JSON.parse(str);
    return typeof parsed === 'object' && parsed !== null ? parsed : null;
  } catch {
    return null;
  }
};

const stringifyLeafValue = (value: unknown): string => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "[]";
  if (typeof value === "object") return "{}";
  return String(value);
};

const TreeField: FC<TreeFieldProps> = ({
  fieldKey,
  value,
  depth = 0,
  defaultExpanded = false,
  path = [],
  isExpanded: isExpandedProp,
  onToggle: onToggleProp
}) => {
  const [internalExpanded, setInternalExpanded] = useState(defaultExpanded);
  const currentPathSegments = useMemo(() => (
    fieldKey ? [...path, fieldKey] : path
  ), [fieldKey, path]);
  const currentPath = useMemo(() => JSON.stringify(currentPathSegments), [currentPathSegments]);

  const expanded = isExpandedProp ? isExpandedProp(currentPath) : internalExpanded;
  const indentWidth = depth * 20;

  const setExpanded = useCallback((newVal: boolean) => {
    if (onToggleProp) {
      onToggleProp(currentPath);
    } else {
      setInternalExpanded(newVal);
    }
  }, [currentPath, onToggleProp]);

  // Parse JSON string once; shared by isExpandable and entries
  const parsedValue = useMemo(() => {
    if (typeof value === 'string') return parseJsonString(value);
    return null;
  }, [value]);

  const entries = useMemo<[string, unknown][]>(() => {
    const target = parsedValue ?? (typeof value === 'object' && value !== null ? value : null);
    return target !== null ? Object.entries(target) : [];
  }, [value, parsedValue]);

  const isExpandable = entries.length > 0;

  const toggle = useCallback((e: Event) => {
    e.stopPropagation();
    setExpanded(!expanded);
  }, [expanded, setExpanded]);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      toggle(e as unknown as Event);
    }
  }, [toggle]);

  // Prevent stack overflow with very deeply nested data — after all hooks
  if (depth >= MAX_TREE_DEPTH) {
    return (
      <div className="vm-tree-field">
        <div className="vm-tree-field__line">
          <span className="vm-tree-field__value">
            <span className="vm-tree-field__value-null">[Max depth reached]</span>
          </span>
        </div>
      </div>
    );
  }

  const isSystemField = fieldKey.startsWith('_');

  const isRoot = !fieldKey;

  return (
    <div
      className={classNames("vm-tree-field", {
        "vm-tree-field_system": isSystemField,
        "vm-tree-field_expanded": expanded,
        "vm-tree-field_root": isRoot
      })}
    >
      {isRoot && !isExpandable && (
        <div className="vm-tree-field__line">
          <span className="vm-tree-field__value">
            {value === null ? (
              <span className="vm-tree-field__value-null">null</span>
            ) : typeof value === 'string' ? (
              <span className="vm-tree-field__value-string">"{value}"</span>
            ) : typeof value === 'number' ? (
              <span className="vm-tree-field__value-number">{String(value)}</span>
            ) : typeof value === 'boolean' ? (
              <span className="vm-tree-field__value-boolean">{String(value)}</span>
            ) : (
              stringifyLeafValue(value)
            )}
          </span>
        </div>
      )}

      {!isRoot && (
        <div className="vm-tree-field__line">
          {depth > 0 && <span className="vm-tree-field__indent" style={{ width: `${indentWidth}px` }}></span>}
          {fieldKey && <span className="vm-tree-field__key">{fieldKey}</span>}

          {isExpandable ? (
            <span>
              {fieldKey && <span className="vm-tree-field__separator">: </span>}
              <span className="vm-tree-field__expander">
                <button
                  className="vm-tree-field__toggle"
                  onClick={toggle}
                  onKeyDown={handleKeyDown}
                  aria-expanded={expanded}
                >
                  {expanded ? '[-]' : '[+]'}
                </button>
              </span>
            </span>
          ) : fieldKey && (
            <span className="vm-tree-field__separator">: </span>
          )}

          {!isExpandable && (
            <span className="vm-tree-field__value">
              {value === null ? (
                <span className="vm-tree-field__value-null">null</span>
              ) : typeof value === 'string' ? (
                <span className="vm-tree-field__value-string">"{value}"</span>
              ) : typeof value === 'number' ? (
                <span className="vm-tree-field__value-number">{String(value)}</span>
              ) : typeof value === 'boolean' ? (
                <span className="vm-tree-field__value-boolean">{String(value)}</span>
              ) : (
                stringifyLeafValue(value)
              )}
            </span>
          )}
        </div>
      )}

      {isExpandable && isRoot && (
        <button
          className="vm-tree-field__root-toggle"
          onClick={toggle}
          onKeyDown={handleKeyDown}
          aria-label={expanded ? "collapse tree" : "expand tree"}
          aria-expanded={expanded}
        >
          {expanded ? '[-]' : '[+]'}
        </button>
      )}

      {isExpandable && expanded && (
        <div className={classNames("vm-tree-field__children", {
          "vm-tree-field__children_root": isRoot
        })}>
          {entries.map(([key, val]) => (
            <TreeField
              key={key}
              fieldKey={key}
              value={val}
              depth={isRoot ? depth : depth + 1}
              path={currentPathSegments}
              isExpanded={isExpandedProp}
              onToggle={onToggleProp}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export default TreeField;
