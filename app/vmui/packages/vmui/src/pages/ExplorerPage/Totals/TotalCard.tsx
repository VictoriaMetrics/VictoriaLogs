import { FC } from "preact/compat";
import { TotalsConfig } from "./totalsConfig";
import "./style.scss";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import { InfoIcon } from "../../../components/Main/Icons";
import Tooltip from "../../../components/Main/Tooltip/Tooltip";

interface Props extends TotalsConfig {
  value: number | string;
  isLoading: boolean;
}

const TotalCard: FC<Props> = ({ title, value = 0, description, formatter, isLoading }) => {

  return (
    <div className="vm-total-card vm-block">

      {isLoading && <LineLoader/>}

      <div className="vm-total-card-header">
        <h3 className="vm-total-card__title vm-title">
          {title}
        </h3>

        <Tooltip title={<div className="vm-total-card-info__text"> {description}</div>}>
          <div className="vm-total-card-info__icon">
            <InfoIcon/>
          </div>
        </Tooltip>
      </div>

      <div className="vm-total-card__value">
        {formatter ? formatter(+value) : value}
      </div>
    </div>
  );
};

export default TotalCard;
