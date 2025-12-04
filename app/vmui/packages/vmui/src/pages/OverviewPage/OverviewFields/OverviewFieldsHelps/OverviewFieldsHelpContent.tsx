import { ctrlKeyLabel } from "../../../../utils/keyboard";

const DocFieldNames = () => (
  <a
    href="https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names"
    target="_blank"
    rel="noopener noreferrer"
  >
    Querying field names
  </a>
);

const DocStreamNames = () => (
  <a
    href="https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-names"
    target="_blank"
    rel="noopener noreferrer"
  >
    Querying stream field names
  </a>
);

const OverviewFieldsHelpContent = () => (
  <div className="vm-overview-fields-tour-content vm-markdown">
    <p>
      This view helps you spot <strong>noisy</strong> and <strong>rare</strong> fields/streams and their
      <strong> values</strong>, and quickly filter the rest.
    </p>

    <hr/>

    <h2>Names table</h2>
    <p>
      Shows field or stream <strong>names</strong> and the number of logs per name.<br/>
      Docs: <DocFieldNames/> and <DocStreamNames/>
    </p>

    <h3>Columns</h3>
    <ul>
      <li><strong>Hits</strong> — number of logs that contain this name (API result).</li>
      <li><strong>Coverage %</strong> — percentage of all logs: <code>hits / total × 100</code>.</li>
    </ul>

    <h3>Click behavior</h3>
    <ul>
      <li>Click a row → selects the name and focuses it (adds a blue filter badge).</li>
      <li><strong>{ctrlKeyLabel} + Click</strong> → applies <strong>Exclude</strong> immediately.</li>
      <li>See <strong>Row actions</strong> for more options.</li>
    </ul>

    <hr/>

    <h2>Values table</h2>
    <p>
      Shows <strong>Top/Bottom N</strong> <strong>values</strong> for the selected name and the number of logs per value.
    </p>

    <h3>Selectors</h3>
    <ul>
      <li><strong>Mode</strong> — <code>Top</code> or <code>Bottom</code>.</li>
      <li><strong>Top N</strong> — how many values to fetch. These controls directly change the query and results.</li>
    </ul>

    <h3>Columns</h3>
    <ul>
      <li><strong>Hits</strong> — count for the specific value.</li>
      <li><strong>% of logs</strong> — percentage of all logs: <code>hits / total × 100</code>.</li>
    </ul>

    <h3>Click behavior</h3>
    <ul>
      <li>Click a row → focuses the value (adds a blue filter badge).</li>
      <li><strong>{ctrlKeyLabel} + Click</strong> → applies <strong>Exclude</strong> immediately.</li>
      <li>See <strong>Row actions</strong>.</li>
    </ul>

    <hr/>

    <h2 id="row-actions">Row actions</h2>
    <ul>
      <li>
        <strong>Focus</strong> — adds a <strong>blue filter badge</strong> and updates <strong>Preview logs</strong>.
        Does <strong>not</strong> change global filters.
      </li>
      <li>
        <strong>Include</strong> — adds a <strong>global include filter</strong> (gray badge) for the selected item.
      </li>
      <li>
        <strong>Exclude</strong> — adds a <strong>global exclude filter</strong> (gray badge) for the selected item.
      </li>
      <li>
        <strong>Copy</strong> — copies the selected <strong>name</strong> (from <em>Names</em>) or the
        <strong> name–value</strong> pair (from <em>Values</em>).
      </li>
    </ul>

    <p>
      <strong>Note:</strong> <strong>Include/Exclude</strong> appear as <strong>gray badges</strong> in
      <strong> Global filters</strong> and affect <strong>all queries on this page</strong> until removed.
    </p>

    <hr/>

    <p><em>* Search and sorting are local (client-side) in both <strong>Names</strong> and <strong>Values</strong> tables.</em></p>
  </div>
);

export default OverviewFieldsHelpContent;
