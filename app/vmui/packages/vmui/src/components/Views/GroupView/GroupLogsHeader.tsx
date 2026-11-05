import { FC, MouseEvent, useCallback, useEffect, useRef, useState } from "preact/compat";
import classNames from "classnames";
import { useSearchParams } from "react-router-dom";
import { useAppState } from "../../../state/common/StateContext";
import useEventListener from "../../../hooks/useEventListener";
import Popper from "../../Main/Popper/Popper";
import useBoolean from "../../../hooks/useBoolean";
import GroupLogsHeaderItem from "./GroupLogsHeaderItem";
import { LOGS_URL_PARAMS } from "../../../constants/logs";
import { GroupLogsType } from "../../../types";
import { formatFieldPair } from "../../../utils/logs";
import useDeviceDetect from "../../../hooks/useDeviceDetect";

interface Props {
  group: GroupLogsType;
  index: number;
  groupByFields: string[];
}

const GroupLogsHeader: FC<Props> = ({ group, index, groupByFields }) => {
  const { isMobile } = useDeviceDetect();
  const { isDarkTheme } = useAppState();
  const [searchParams] = useSearchParams();

  const containerRef = useRef<HTMLDivElement>(null);
  const moreRef = useRef<HTMLDivElement>(null);

  const {
    value: openMore,
    toggle: handleToggleMore,
    setFalse: handleCloseMore,
  } = useBoolean(false);

  const [hideParisCount, setHideParisCount] = useState<number>(0);

  const compactGroupHeader = isMobile || searchParams.get(LOGS_URL_PARAMS.COMPACT_GROUP_HEADER) === "true";

  const withFieldName = groupByFields.length > 1;

  const pairs = group.pairs;
  const hideAboveIndex = pairs.length - hideParisCount - 1;

  const handleClickMore = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation();
    handleToggleMore();
  };

  const calcVisiblePairsCount = useCallback(() => {
    if (!compactGroupHeader || !containerRef.current) {
      setHideParisCount(0);
      return;
    }

    const container = containerRef.current;
    const containerSize = container.getBoundingClientRect();
    const selector = ".vm-group-logs-section-keys__pair:not(.vm-group-logs-section-keys__pair_more)";
    const children = Array.from(container.querySelectorAll(selector));
    let count = 0;

    for (const child of children) {
      const { right } = (child as HTMLElement).getBoundingClientRect();

      if ((right + 220) > containerSize.width) {
        count++;
      }
    }

    setHideParisCount(count);
  }, [compactGroupHeader, containerRef]);

  useEffect(calcVisiblePairsCount, [group.pairs, compactGroupHeader, containerRef]);

  useEventListener("resize", calcVisiblePairsCount);

  return (
    <div
      className={classNames({
        "vm-group-logs-section-keys": true,
        "vm-group-logs-section-keys_compact": compactGroupHeader,
      })}
      ref={containerRef}
    >
      <span
        className="vm-group-logs-section-keys__title"
        title={`Group by ${groupByFields.join(", ")}`}
      >
        {index + 1}. Group by <code>{groupByFields.join(", ")}</code>
      </span>
      {pairs.map((pair, i) => (
        <GroupLogsHeaderItem
          key={`${i}_${formatFieldPair(pair)}`}
          pair={pair}
          withFieldName={withFieldName}
          isHide={hideParisCount ? i > hideAboveIndex : false}
        />
      ))}
      {hideParisCount > 0 && (
        <>
          <div
            className={classNames({
              "vm-group-logs-section-keys__pair": true,
              "vm-group-logs-section-keys__pair_more": true,
              "vm-group-logs-section-keys__pair_dark": isDarkTheme
            })}
            ref={moreRef}
            onClick={handleClickMore}
          >
            +{hideParisCount} more
          </div>
          <Popper
            open={openMore}
            buttonRef={moreRef}
            placement="bottom-left"
            onClose={handleCloseMore}
          >
            <div className="vm-group-logs-section-keys vm-group-logs-section-keys_popper">
              {pairs.slice(hideAboveIndex + 1).map((pair, i) => (
                <GroupLogsHeaderItem
                  key={`${i}_${formatFieldPair(pair)}`}
                  pair={pair}
                  withFieldName={withFieldName}
                />
              ))}
            </div>
          </Popper>
        </>
      )}
      <span className="vm-group-logs-section-keys__count">{group.total} entries</span>
    </div>
  )
  ;
};

export default GroupLogsHeader;
