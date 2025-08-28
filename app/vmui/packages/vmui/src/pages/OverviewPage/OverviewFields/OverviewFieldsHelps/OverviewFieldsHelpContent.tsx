const DocFieldNames = () => (
  <a
    href="https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names"
    target="_blank"
    rel="noopener noreferrer"
  >Querying field names</a>
);

const DocStreamNames = () => (
  <a
    href="https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names"
    target="_blank"
    rel="noopener noreferrer"
  >Querying field names</a>
);

const OverviewFieldsHelpContent = () => (
  <div className="vm-overview-fields-tour-content vm-markdown">
    <p>This view helps you find <strong>noisy</strong> and <strong>rare</strong> fields/streams and
      their <strong>values</strong>, and quickly filter the rest.</p>

    <hr/>

    <h2>Names table</h2>
    <p>
      Shows field or stream <strong>names</strong>, also contains the number of log results per every field name.<br/>
      Docs: <DocFieldNames/> and <DocStreamNames/>
    </p>

    <h3>Columns</h3>
    <ul>
      <li><strong>Hits</strong> — number of logs that contain this name (from API).</li>
      <li><strong>Coverage %</strong> — share of all logs that contain this name: <code>hits / total × 100</code>.</li>
    </ul>

    <h3>Click behavior</h3>
    <ul>
      <li>Click a row → selects the name and focuses it (adds a blue filter badge).</li>
      <li><strong>Ctrl/Cmd + Click</strong> → applies <strong>Exclude</strong> immediately.</li>
      <li>See <strong>Row actions</strong> for menu options.</li>
    </ul>

    <hr/>

    <h2>Values table</h2>
    <p>Shows <strong>Top/Bottom N</strong> <strong>values</strong> for the selected name, also contains the number of
      log results per every value.</p>

    <h3>Selectors</h3>
    <ul>
      <li><strong>Mode</strong> — <code>Top</code> or <code>Bottom</code>.</li>
      <li><strong>Top N</strong> — how many values to fetch.<br/>
        These controls directly change the query and results.
      </li>
    </ul>

    <h3>Columns</h3>
    <ul>
      <li><strong>Hits</strong> — count for the specific value.</li>
      <li><strong>% of logs</strong> — share of all logs: <code>hits / total × 100</code>.</li>
    </ul>

    <h3>Click behavior</h3>
    <ul>
      <li>Click a row → focuses the value (adds a blue filter badge).</li>
      <li><strong>Ctrl/Cmd + Click</strong> → applies <strong>Exclude</strong> immediately.</li>
      <li>See <strong>Row actions</strong>.</li>
    </ul>

    <hr/>

    <h2 id="row-actions">Row actions</h2>
    <ul>
      <li><strong>Focus</strong> — adds a <strong>blue filter badge</strong> and updates <strong>Preview logs</strong>.
        Does <strong>not</strong> change global filters.
      </li>
      <li><strong>Include</strong> — on a <strong>name</strong> → <code>field:*</code>; on
        a <strong>value</strong> → <code>field:value</code>.
      </li>
      <li><strong>Exclude</strong> — on a <strong>name</strong> → <code>(NOT field:*)</code>; on
        a <strong>value</strong> → <code>(NOT field:value)</code>.
      </li>
      <li><strong>Copy</strong> — copies the <strong>name</strong> (from <em>Names</em>) or
        the <code>name:value</code> pair (from <em>Values</em>).
      </li>
    </ul>

    <p><strong>Note:</strong> <strong>Include/Exclude</strong> appear as <strong>gray badges</strong> in <strong>Global
      filters</strong> and affect <strong>all queries on this page</strong> until removed.</p>

    <hr/>

    <p><em>* Search and sorting are local (client-side) in
      both <strong>Names</strong> and <strong>Values</strong> tables.</em></p>
  </div>

);

export default OverviewFieldsHelpContent;
