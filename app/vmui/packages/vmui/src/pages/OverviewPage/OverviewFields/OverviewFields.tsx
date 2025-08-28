import { FC } from "preact/compat";
import { useState } from "react";
import Tabs from "../../../components/Main/Tabs/Tabs";
import classNames from "classnames";
import TopFieldNames from "./OverviewFieldsTables/TopFieldNames";
import TopFieldValues from "./OverviewFieldsTables/TopFieldValues";
import TopStreamNames from "./OverviewFieldsTables/TopStreamNames";
import "../OverviewTable/style.scss";
import FieldCardinalityCard from "./FieldCardinalityCard/FieldCardinalityCard";
import OverviewFieldsHelps from "./OverviewFieldsHelps/OverviewFieldsHelps";

const overviewTabs = [
  {
    label: "Fields",
    value: "fields",
    component: (
      <div className="vm-top-fields-row">
        <TopFieldNames/>
        <div>
          <FieldCardinalityCard scope={"field"}/>
          <TopFieldValues scope={"field"}/>
        </div>
      </div>
    )
  },
  {
    label: "Streams",
    value: "streams",
    component: (
      <div className="vm-top-fields-row">
        <TopStreamNames/>
        <div>
          <FieldCardinalityCard scope={"stream"}/>
          <TopFieldValues scope={"stream"}/>
        </div>
      </div>
    )
  },
];

const OverviewFields: FC = () => {
  const [activeTab, setActiveTab] = useState(overviewTabs[0].value);

  return (
    <div className="vm-explorer-page-panel vm-block">
      <div className="vm-section-header vm-explorer-page-panel-header">
        <div className="vm-section-header__tabs">
          <Tabs
            activeItem={activeTab}
            items={overviewTabs}
            onChange={setActiveTab}
          />
        </div>

        <div>
          <OverviewFieldsHelps/>
        </div>
      </div>

      {overviewTabs.map((tab) => (
        <div
          key={tab.value}
          className={classNames({
            "vm-explorer-page-panel-content": true,
            "vm-explorer-page-panel-content_active": tab.value === activeTab,
          })}
        >
          {tab.component}
        </div>
      ))}
    </div>
  );
};

export default OverviewFields;
