import { FC, useMemo } from "preact/compat";
import { TotalsConfig } from "./totalsConfig";
import "./style.scss";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import { InfoIcon } from "../../../components/Main/Icons";
import Tooltip from "../../../components/Main/Tooltip/Tooltip";
import classNames from "classnames";
import { TimeParams } from "../../../types";
import dayjs from "dayjs";
import { DATE_TIME_FORMAT } from "../../../constants/date";

interface Props extends TotalsConfig {
  isLoading: boolean;
  value?: string;
  valuePrev?: string;
  periods?: {
    curr: TimeParams
    prev: TimeParams
  }
}

const TotalCard: FC<Props> = ({ title, value, valuePrev, description, formatter, isLoading, periods }) => {
  const { curr, delta, deltaPct } = useMemo(() => {
    const currNum = Number(value ?? 0);
    const prevNum = valuePrev === undefined ? NaN : Number(valuePrev);

    const hasPrev = !Number.isNaN(prevNum);
    const abs = hasPrev ? currNum - prevNum : NaN;
    const pct = hasPrev && prevNum !== 0 ? abs / prevNum : NaN;

    return {
      curr: currNum,
      deltaPct: Math.abs(pct),
      delta: abs
    };
  }, [value, valuePrev]);

  const prevTimeRange = useMemo(() => {
    if (!periods?.prev) return "";
    const { start, end } = periods.prev;
    const startStr = dayjs(start * 1000).tz().format(DATE_TIME_FORMAT);
    const endStr = dayjs(end * 1000).tz().format(DATE_TIME_FORMAT);
    return `${startStr} — ${endStr}`;
  }, [periods]);

  return (
    <div className="vm-total-card vm-block">
      {isLoading && <LineLoader/>}

      <div className="vm-total-card-header">
        <h3 className="vm-total-card__title vm-title">{title}</h3>
        <Tooltip
          title={<div className="vm-total-card-info__text">{description}</div>}
          placement="bottom-right"
        >
          <div className="vm-total-card-info__icon"><InfoIcon/></div>
        </Tooltip>
      </div>

      <div className="vm-total-card-body">
        <div className="vm-total-card__value">
          {formatter ? formatter(curr) : curr}
        </div>

        {!isNaN(deltaPct) && !!deltaPct && (
          <Tooltip
            title={(
              <div>
                <p>Change compared to the previous time range:</p>
                {!!prevTimeRange && <p>{prevTimeRange}</p>}
              </div>
            )}
          >
            <div
              className={classNames({
                "vm-dynamic-number vm-total-card__delta": true,
                "vm-dynamic-number_positive vm-dynamic-number_down": delta < 0,
                "vm-dynamic-number_negative vm-dynamic-number_up": delta > 0,
              })}
            >
              {(deltaPct * 100).toFixed(2)}%
            </div>
          </Tooltip>
        )}
      </div>
    </div>
  );
};

export default TotalCard;
