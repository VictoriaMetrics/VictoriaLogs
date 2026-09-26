import { FC } from "preact/compat";
import "uplot/dist/uPlot.min.css";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import { formatNumberShort, formatNumber } from "../../../../utils/number";
import { formatRequestDuration } from "../../../../utils/time";
import "./style.scss";

interface Props {
  totalHits: number;
  isHitsMode: boolean
  durationMs?: number;
}

const BarHitsStats: FC<Props> = ({ totalHits, isHitsMode, durationMs }) => {
  const { isMobile } = useDeviceDetect();

  const totalHitsFormat = isMobile ? formatNumberShort(totalHits) : formatNumber(totalHits);
  const durationFormat = durationMs ? formatRequestDuration(durationMs) : null;

  if (!isHitsMode && !durationFormat) return null;

  return (
    <div className="vm-bar-hits-stats">
      {isHitsMode && (
        <p className="vm-bar-hits-stats__item">
          Total: <b>{totalHitsFormat}</b>
        </p>
      )}
      {durationFormat && (
      <p className="vm-bar-hits-stats__query-time">
        Hits query ({durationFormat})
      </p>
      )}
    </div>
  );
};

export default BarHitsStats;
