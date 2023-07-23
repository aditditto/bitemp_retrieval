import AVLTree from "./avl";

function tmda_fi(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames, p_interval) {
  // Dependencies
  function toIsoString(date) {
    var tzo = -date.getTimezoneOffset(),
        dif = tzo >= 0 ? '+' : '-',
        pad = function(num) {
            return (num < 10 ? '0' : '') + num;
        };
  
    return date.getFullYear() +
        '-' + pad(date.getMonth() + 1) +
        '-' + pad(date.getDate()) +
        'T' + pad(date.getHours()) +
        ':' + pad(date.getMinutes()) +
        ':' + pad(date.getSeconds()) +
        dif + pad(Math.floor(Math.abs(tzo) / 60)) +
        ':' + pad(Math.abs(tzo) % 60);
  }
  const log_level = DEBUG1;
  const vars = [p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames, p_interval];
  vars.forEach(el => plv8.elog(log_level, `function param: ${el}`));
  // PREP: Initialize gt and index
  eval(plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'")[0].source);

  // Function for AVL tree insertion, assumes key is range endpoint so NaN is +infinity
  const tmdaDateComparator = function(a, b) {
    if (isNaN(a) && isNaN(b)) return 0;
    if (isNaN(a)) return 1;
    if (isNaN(b)) return -1;

    return a > b ? 1 : a < b ? -1 : 0;
  }

  // SETUP
  const v_group_by_q = `SELECT column_name, data_type FROM information_schema.columns WHERE 
  table_schema='${p_schema}' AND table_name='${p_table}' AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`;
  plv8.elog(log_level, v_group_by_q);
  var v_group_by = plv8.execute(v_group_by_q);
  plv8.elog(log_level, `v_group_by: ${v_group_by}`);

  v_group_by.forEach(col => {
    col.column_name = `"${col.column_name}"`
  });
  let v_group_by_cols = v_group_by.map(col => col.column_name);
  let v_aggr_target_cols = p_aggr_target.map(col => `"${col}"`);

  const create_temp_table_q = `CREATE TEMP TABLE tmda_fi_aggr_group(
    id SERIAL PRIMARY KEY,
    ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
    effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
    )`;
  plv8.elog(log_level, create_temp_table_q);
  plv8.execute(create_temp_table_q);
  plv8.execute(`CREATE INDEX lookup_tmda ON tmda_fi_aggr_group (${v_group_by_cols.join(', ')})`);

  plv8.execute('ANALYZE tmda_ci_aggr_group');

  plv8.elog(log_level, "PREP DONE");
  // COMPUTE: Iterate rows and compute result set

  var v_attr_props_query = plv8.execute(`SELECT attr_name, attr_property FROM bitemporal_internal.temporal_attribute_properties
  WHERE schema_name=$1 AND table_name=$2 AND attr_name IN (${p_aggr_target.map(x => `'${x}'`).join(', ')})`,
  [p_schema, p_table]);
  var v_attr_props = new Map();

  v_attr_props_query.forEach(col => v_attr_props.set(col.attr_name, col.attr_property));
  plv8.elog(log_level, `v_attr_props: ${[...v_attr_props.entries()]}`);

  const plan = plv8.prepare(`SELECT 
  ${v_group_by_cols.join(", ")},
  ${v_aggr_target_cols.join(", ")},
  LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
  FROM ${`"${p_schema}"."${p_table}"`}
  ORDER BY LOWER(effective)`);
  const cursor = plan.cursor();

  let rows = cursor.fetch(5);

  while (rows) {
    // 

    rows = cursor.fetch(5);
  }


  plv8.execute('DROP TABLE tmda_fi_aggr_group');
}
