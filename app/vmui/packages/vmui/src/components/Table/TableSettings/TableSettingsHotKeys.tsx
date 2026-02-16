import { FC } from "preact/compat";

const TableSettingsHotKeys: FC = () => {

  return (
    <div className="vm-table-settings-section-search-help-tooltip">
      <p className="vm-table-settings-section-search-help-tooltip-item">
        <span>↑</span> <span>Move focus up</span>
      </p>
      <p className="vm-table-settings-section-search-help-tooltip-item">
        <span>↓</span> <span>Move focus down</span>
      </p>
      <p className="vm-table-settings-section-search-help-tooltip-item">
        <span>Enter</span> <span>Toggle selected/unselected for focused field</span>
      </p>
      <p className="vm-table-settings-section-search-help-tooltip-item">
        <span>Shift + ↑</span> <span>Move selected field up (reorder)</span>
      </p>
      <p className="vm-table-settings-section-search-help-tooltip-item">
        <span>Shift + ↓</span> <span>Move selected field down (reorder)</span>
      </p>
      <p className="vm-table-settings-section-search-help-tooltip-item">
        <span>Esc</span> <span>Close table setting</span>
      </p>
    </div>
  );
};

export default TableSettingsHotKeys;
