--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitemp_retrieval" to load this file. \quit

CREATE OR REPLACE
FUNCTION get_sum(int, int) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT
SET search_path = 'temporal_relationships';

CREATE OR REPLACE
FUNCTION interval_len(
    interv temporal_relationships.timeperiod
    ) 
RETURNS INTERVAL
AS
$BODY$
BEGIN
  IF UPPER(interv)='infinity' THEN
    RETURN age(now(), LOWER(interv));
  ELSE
    RETURN age(UPPER(interv), LOWER(interv));
  END IF;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION bitemp_contains_ts(
    effective_range temporal_relationships.timeperiod,
    asserted_range temporal_relationships.timeperiod,
    effective_ts timestamptz,
    asserted_ts timestamptz
    ) 
RETURNS BOOLEAN
AS
$BODY$
BEGIN
  RETURN effective_ts <@ effective_range AND asserted_ts <@ asserted_range;  
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION bitemp_contains_now(
    effective_range temporal_relationships.timeperiod,
    asserted_range temporal_relationships.timeperiod
    ) 
RETURNS BOOLEAN
AS
$BODY$
BEGIN
  RETURN now() <@ effective_range AND now() <@ asserted_range;  
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION interval_contains_ts(
    interv temporal_relationships.timeperiod,
    ts timestamptz
    ) 
RETURNS BOOLEAN
AS
$BODY$
BEGIN
  RETURN ts <@ interv;  
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION interval_contains_now(
    interv temporal_relationships.timeperiod
    ) 
RETURNS BOOLEAN
AS
$BODY$
BEGIN
  RETURN now() <@ interv;  
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION interval_joinable(
    a temporal_relationships.timeperiod,
    b temporal_relationships.timeperiod
    ) 
RETURNS BOOLEAN
AS
$BODY$
BEGIN
  RETURN temporal_relationships.has_includes(a, b);
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION interval_join(
    a temporal_relationships.timeperiod,
    b temporal_relationships.timeperiod
    ) 
RETURNS temporal_relationships.timeperiod
AS
$BODY$
BEGIN
  RETURN a * b;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION unitemp_coalesce_select(
    p_query TEXT,
    p_list_of_fields TEXT[],
    p_temporal_field TEXT
)
RETURNS SETOF RECORD
AS
$body$
  function parseISOString(s) {
    var b = s.split(/\D+/);
    return new Date(Date.UTC(b[0], --b[1], b[2], b[3], b[4], b[5], b[6]));
  }

  plv8.execute("SET DATESTYLE TO 'ISO'");
  let fields_string = p_list_of_fields.map((field) => `"${field}"`).join(",");
  let query = `${p_query} ORDER BY ${fields_string}, LOWER("${p_temporal_field}")`;
  let plan = plv8.prepare(query);
  let cursor = plan.cursor();

  let curr_row, to_coalesce, coalesce_start, coalesce_end, endpoints;

  while ((curr_row = cursor.fetch())) {
    endpoints = curr_row[p_temporal_field].slice(1, -1).split(",");

    if (!to_coalesce) {
      to_coalesce = curr_row;
      coalesce_start = endpoints[0];
      coalesce_end = endpoints[1];
    } else {
      let equal = true;
      for (field of p_list_of_fields) {
        if (curr_row[field] != to_coalesce[field]) {
          equal = false;
        }
      }

      if (equal) {
        if (coalesce_end != "infinity") {
          let coalesce_end_date = parseISOString(coalesce_end);
          let curr_start_date = parseISOString(endpoints[0]);

          if (curr_start_date <= coalesce_end_date) {
            coalesce_end = endpoints[1];
          } else {
            to_coalesce[
              p_temporal_field
            ] = `[${coalesce_start},${coalesce_end})`;
            plv8.return_next(to_coalesce);

            to_coalesce = curr_row;
            coalesce_start = endpoints[0];
            coalesce_end = endpoints[1];
          }
        }
      } else {
        to_coalesce[p_temporal_field] = `[${coalesce_start},${coalesce_end})`;
        plv8.return_next(to_coalesce);

        to_coalesce = curr_row;
        coalesce_start = endpoints[0];
        coalesce_end = endpoints[1];
      }
    }
  }

  to_coalesce[p_temporal_field] = `[${coalesce_start},${coalesce_end})`;
  plv8.return_next(to_coalesce);

  cursor.close();
  plan.free();
$body$
LANGUAGE plv8;


CREATE OR REPLACE
FUNCTION unitemp_coalesce_select_effective(
    p_query TEXT,
    p_list_of_fields TEXT[]
)
RETURNS SETOF RECORD
AS
$body$
  function parseISOString(s) {
    var b = s.split(/\D+/);
    return new Date(Date.UTC(b[0], --b[1], b[2], b[3], b[4], b[5], b[6]));
  }

  plv8.execute("SET DATESTYLE TO 'ISO'");
  let p_temporal_field = "effective";
  let fields_string = p_list_of_fields.map((field) => `"${field}"`).join(",");
  let query = `${p_query} ORDER BY ${fields_string}, LOWER("${p_temporal_field}")`;
  let plan = plv8.prepare(query);
  let cursor = plan.cursor();

  let curr_row, to_coalesce, coalesce_start, coalesce_end, endpoints;

  while ((curr_row = cursor.fetch())) {
    endpoints = curr_row[p_temporal_field].slice(1, -1).split(",");

    if (!to_coalesce) {
      to_coalesce = curr_row;
      coalesce_start = endpoints[0];
      coalesce_end = endpoints[1];
    } else {
      let equal = true;
      for (field of p_list_of_fields) {
        if (curr_row[field] != to_coalesce[field]) {
          equal = false;
        }
      }

      if (equal) {
        if (coalesce_end != "infinity") {
          let coalesce_end_date = parseISOString(coalesce_end);
          let curr_start_date = parseISOString(endpoints[0]);

          if (curr_start_date <= coalesce_end_date) {
            coalesce_end = endpoints[1];
          } else {
            to_coalesce[
              p_temporal_field
            ] = `[${coalesce_start},${coalesce_end})`;
            plv8.return_next(to_coalesce);

            to_coalesce = curr_row;
            coalesce_start = endpoints[0];
            coalesce_end = endpoints[1];
          }
        }
      } else {
        to_coalesce[p_temporal_field] = `[${coalesce_start},${coalesce_end})`;
        plv8.return_next(to_coalesce);

        to_coalesce = curr_row;
        coalesce_start = endpoints[0];
        coalesce_end = endpoints[1];
      }
    }
  }

  to_coalesce[p_temporal_field] = `[${coalesce_start},${coalesce_end})`;
  plv8.return_next(to_coalesce);

  cursor.close();
  plan.free();
$body$
LANGUAGE plv8;

CREATE OR REPLACE
FUNCTION unitemp_coalesce_table_effective(
    p_schema TEXT,
    p_table TEXT,
    p_list_of_fields TEXT[]
)
RETURNS SETOF RECORD
AS
$body$
  function parseISOString(s) {
    var b = s.split(/\D+/);
    return new Date(Date.UTC(b[0], --b[1], b[2], b[3], b[4], b[5], b[6]));
  }

  plv8.execute("SET DATESTYLE TO 'ISO'");
  let p_temporal_field = "effective";
  let fields_string = p_list_of_fields.map((field) => `"${field}"`).join(",");
  let query = `SELECT ${fields_string}, \"${p_temporal_field}\" FROM \"${p_schema}\".\"${p_table}\" WHERE now() <@ asserted ORDER BY ${fields_string}, \"${p_temporal_field}\"`;
  let plan = plv8.prepare(query);
  let cursor = plan.cursor();

  let curr_row, to_coalesce, coalesce_start, coalesce_end, endpoints;

  while ((curr_row = cursor.fetch())) {
    endpoints = curr_row[p_temporal_field].slice(1, -1).split(",");

    if (!to_coalesce) {
      to_coalesce = curr_row;
      coalesce_start = endpoints[0];
      coalesce_end = endpoints[1];
    } else {
      let equal = true;
      for (field of p_list_of_fields) {
        if (curr_row[field] != to_coalesce[field]) {
          equal = false;
        }
      }

      if (equal) {
        if (coalesce_end != "infinity") {
          let coalesce_end_date = parseISOString(coalesce_end);
          let curr_start_date = parseISOString(endpoints[0]);

          if (curr_start_date <= coalesce_end_date) {
            coalesce_end = endpoints[1];
          } else {
            to_coalesce[
              p_temporal_field
            ] = `[${coalesce_start},${coalesce_end})`;
            plv8.return_next(to_coalesce);

            to_coalesce = curr_row;
            coalesce_start = endpoints[0];
            coalesce_end = endpoints[1];
          }
        }
      } else {
        to_coalesce[p_temporal_field] = `[${coalesce_start},${coalesce_end})`;
        plv8.return_next(to_coalesce);

        to_coalesce = curr_row;
        coalesce_start = endpoints[0];
        coalesce_end = endpoints[1];
      }
    }
  }

  to_coalesce[p_temporal_field] = `[${coalesce_start},${coalesce_end})`;
  plv8.return_next(to_coalesce);

  cursor.close();
  plan.free();
$body$
LANGUAGE plv8;



CREATE FUNCTION v8_select_test(p_query TEXT, p_interval INTERVAL, p_list TEXT[]) RETURNS void AS 
$$
    var avl = plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'");
    eval(avl[0].source);
    var minfinity = plv8.execute(`select lower('["-infinity", "infinity"]'::tstzrange), upper('["-infinity", "infinity"]'::tstzrange);`);
    plv8.elog(INFO, minfinity[0].lower.getTime());
    plv8.elog(INFO, minfinity[0].upper.getTime());
    plv8.elog(INFO, minfinity[0].lower > Date.now());
    plv8.elog(INFO, minfinity[0].lower < Date.now());
    var tsrange = plv8.execute(`select '["-infinity", "infinity"]'::tstzrange AS test`);
    plv8.elog(INFO, tsrange[0].test);
    plv8.elog(INFO, typeof tsrange[0].test);
    plv8.elog(INFO, plv8.execute(`select extract(epoch from interval '${p_interval}')`)[0].extract);
    plv8.elog(INFO, p_interval);
    plv8.elog(INFO, typeof p_interval);
    plv8.elog(INFO, p_list.length);
$$
LANGUAGE plv8;

CREATE FUNCTION v8_infinity_test() RETURNS tstzrange AS 
$$
    var minfinity = plv8.execute(`select lower('["-infinity", "infinity"]'::tstzrange), upper('["-infinity", "infinity"]'::tstzrange);`);
    plv8.elog(INFO, minfinity[0].lower.getTime());
    plv8.elog(INFO, minfinity[0].upper.getTime());
    plv8.elog(INFO, minfinity[0].lower > Date.now());
    plv8.elog(INFO, minfinity[0].lower < Date.now());

    var test_select_props = plv8.execute(`SELECT * FROM bitemporal_internal.temporal_attribute_properties`);
    plv8.elog(INFO, test_select_props[0].attr_property == 'malleable');
    plv8.elog(INFO, test_select_props[0].attr_property === 'malleable');

    return `["${new Date().toISOString()}", "infinity"]`;
$$
LANGUAGE plv8;

CREATE FUNCTION tmda_ci_now(
  p_schema TEXT,
  p_table TEXT,
  p_group_by TEXT[],
  p_aggr_funcs TEXT[],
  p_aggr_target TEXT[],
  p_aggr_fieldnames TEXT[]) 
RETURNS SETOF RECORD AS 
$$
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
    const vars = [p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames];
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

    const v_group_by_q = `SELECT column_name, data_type FROM information_schema.columns WHERE 
    table_schema='${p_schema}' AND table_name='${p_table}' AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`;
    if (p_group_by.length > 0) {
      plv8.elog(log_level, v_group_by_q);
      var v_group_by = plv8.execute(v_group_by_q);
      plv8.elog(log_level, `v_group_by: ${v_group_by}`);

      v_group_by.forEach(col => {
        col.column_name = `"${col.column_name}"`
      });
      var v_group_by_cols = v_group_by.map(col => col.column_name);
    }
    let v_aggr_target_cols = p_aggr_target.map(col => `"${col}"`);
    const create_temp_table_q = 
      p_group_by.length == 0 ? 
        `CREATE TEMP TABLE tmda_ci_aggr_group(
        id SERIAL PRIMARY KEY,
        effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
        )`
        : 
        `CREATE TEMP TABLE tmda_ci_aggr_group(
        id SERIAL PRIMARY KEY,
        ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
        effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
        )`;
    plv8.elog(log_level, create_temp_table_q);
    plv8.execute(create_temp_table_q);
    if (p_group_by.length > 0) {
      plv8.execute(`CREATE INDEX lookup_tmda ON tmda_ci_aggr_group (${v_group_by_cols.join(', ')})`);
    }
    const gt_insert_q = 
    p_group_by.length == 0 ? 
    `WITH rows AS (
      INSERT INTO tmda_ci_aggr_group(id) VALUES (1)
      RETURNING 1
    ) SELECT count(*) AS rownum FROM rows`
    : 
    `WITH rows AS (
      INSERT INTO tmda_ci_aggr_group(${v_group_by_cols.join(', ')})
    (
      SELECT DISTINCT
        ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}
      FROM ${`"${p_schema}"."${p_table}"`}
    ) RETURNING 1
    ) SELECT count(*) AS rownum FROM rows`;
    let group_table_count = plv8.execute(gt_insert_q)[0].rownum;
    plv8.elog(log_level, `gt_insert_q: ${gt_insert_q} gt_rowcount: ${group_table_count}`);

    // const debug_gt = plv8.execute(`SELECT * FROM tmda_ci_aggr_group`);
    // for (let i = 0; i < debug_gt.length; i++) {
    //   const g = debug_gt[i];
    //   plv8.elog(log_level, `g[${i}]: ${JSON.stringify(g)}`);
    // }

    plv8.execute('ANALYZE tmda_ci_aggr_group');

    plv8.elog(log_level, "PREP DONE");
    // COMPUTE: Iterate rows and compute result set

    var v_attr_props_query = plv8.execute(`SELECT attr_name, attr_property FROM bitemporal_internal.temporal_attribute_properties
    WHERE schema_name=$1 AND table_name=$2 AND attr_name IN (${p_aggr_target.map(x => `'${x}'`).join(', ')})`,
    [p_schema, p_table]);
    var v_attr_props = new Map();

    v_attr_props_query.forEach(col => v_attr_props.set(col.attr_name, col.attr_property));
    plv8.elog(log_level, `v_attr_props: ${[...v_attr_props.entries()]}`);

    let avltrees = [];
    for (let i = 0; i < group_table_count; i++)
      avltrees.push(new AVLTree(tmdaDateComparator, true));

    const plan_q =
      p_group_by.length == 0 ?
      `SELECT
      ${v_aggr_target_cols.join(", ")},
      LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
      FROM ${`"${p_schema}"."${p_table}"`} WHERE now() <@ asserted
      ORDER BY LOWER(effective)`
      :
      `SELECT 
      ${v_group_by_cols.join(", ")},
      ${v_aggr_target_cols.join(", ")},
      LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
      FROM ${`"${p_schema}"."${p_table}"`} WHERE now() <@ asserted
      ORDER BY LOWER(effective)`;
    const plan = plv8.prepare(plan_q);
    const cursor = plan.cursor();

    let rows = cursor.fetch(5);
    while (rows) { // foreach tuple r ∈ r in chronological order
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const gt_lookup_q = 
        p_group_by.length == 0 ? 
        `SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group`
        :
        `SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group WHERE
        ${v_group_by_cols.map(col => `${col}='${row[col.slice(1, -1)]}'`).join(" AND ")}`;
        plv8.elog(log_level, gt_lookup_q);
        const gt_lookup = plv8.execute(gt_lookup_q);

        for (let j = 0; j < gt_lookup.length; j++) { // foreach i ∈ LOOKUP(gt, r, θ)
          const g = gt_lookup[j];
          const tree = avltrees[g.id-1];

          if (isNaN(g.tstart) || row.effective_start > g.tstart) { // Check -infinity or r.ts > gt[i].ts
            // const startminusone = new Date(row.effective_start.getTime()-1); // Insert r.Ts-1
            if (!tree.find(row.effective_start)) tree.insert(row.effective_start, []);

            let toDelete = [];
            tree.forEach(function(node) { // foreach v ∈ gt[i].T in chronological order
              if (node.key <= row.effective_start) { // where v.t < r.Ts
                g.tend = node.key; // gt[i].Te ← v.t;

                let aggrResults = new Array(p_aggr_funcs.length).fill(0); // ResultTuple helpers
                let counts = new Array(p_aggr_funcs.length).fill(0);
                tree.forEach(function(resultmember) { // ResultTuple(gt[i], F, C)
                  const nodeRows = resultmember.data;
                  for (let noderowCount = 0; noderowCount < nodeRows.length; noderowCount++) {
                    const noderow = nodeRows[noderowCount];
                    
                    for (let k = 0; k < p_aggr_funcs.length; k++) {
                      // we need: aggr_func, rowname, property
                      const aggr_func = p_aggr_funcs[k];
                      let attr_scaling = 1;

                      if (v_attr_props.get(p_aggr_target[k]) == 'malleable' || v_attr_props.get(p_aggr_target[k]) == 'atomic') {
                        let rowtend = isNaN(noderow.effective_end.getTime()) ? Date.now() : noderow.effective_end.getTime();
                        plv8.elog(log_level, `rowtend: ${rowtend}`);
                        plv8.elog(log_level, `g.tstart.getTime(): ${g.tstart.getTime()}`);
                        plv8.elog(log_level, `noderow.effective_start.getTime(): ${noderow.effective_start.getTime()}`);
                        attr_scaling = (g.tend.getTime() - g.tstart.getTime()) / (rowtend - noderow.effective_start.getTime());
                      }
                      plv8.elog(log_level, `p_aggr_target[k]: ${p_aggr_target[k]}`);
                      plv8.elog(log_level, `attr_scaling: ${attr_scaling}`);

                      if (!(v_attr_props.get(p_aggr_target[k]) == 'atomic' && attr_scaling != 1)) {
                        counts[k]++;
                        const rowval = noderow[p_aggr_target[k]] * attr_scaling;
                      
                        if (aggr_func == "avg") {
                          aggrResults[k] += rowval;
                        } else if (aggr_func == "sum") {
                          aggrResults[k] += rowval;
                        } else if (aggr_func == "max") {
                          if (counts[k] === 1) { // First row
                            aggrResults[k] = rowval;
                          } else {
                            if (rowval > aggrResults[k]) {
                              aggrResults[k] = rowval;
                            }
                          }
                        } else if (aggr_func == "min") {
                          if (counts[k] === 1) {
                            aggrResults[k] = rowval;
                          } else {
                            if (rowval < aggrResults[k]) {
                              aggrResults[k] = rowval;
                            }
                          }
                        } else if (aggr_func == "count") {
                            aggrResults[k]++;
                        }
                      }
                    }
                  }
                });
                
                let meaningful = false;
                for (let k = 0; k < p_aggr_funcs.length; k++) {
                  if (counts[k] > 0) {
                    meaningful = true;
                  }
      
                  const aggr_func = p_aggr_funcs[k];
                  if (aggr_func == "avg") {
                    aggrResults[k] = aggrResults[k] / counts[k];
                  }
                }
      
                if (meaningful) {
                  const result_period = `["${toIsoString(g.tstart)}", "${isNaN(g.tend) ? 'infinity' : toIsoString(g.tend)}")`
                  let to_return = {};
                  for (let k = 0; k < p_aggr_fieldnames.length; k++) {
                    const fieldname = p_aggr_fieldnames[k];
                    if (counts[k] > 0) {
                      to_return[fieldname] = aggrResults[k];
                    } else {
                      to_return[fieldname] = null;
                    }
                  }
      
                  for (let k = 0; k < p_group_by.length; k++) {
                    const fieldname = p_group_by[k];
                    to_return[fieldname] = g[fieldname];
                  }
      
                  to_return.effective = result_period;
                  plv8.return_next(to_return);
                }

                plv8.elog(log_level, `node.key: ${node.key}`);
                g.tstart = node.key; // gt[i].T ← [v.t + 1, ∗];
                g.tend = new Date(NaN); 
                // plv8.elog(log_level, `g.tstart: ${g.tstart}, g.tend: ${g.tend}`);

                node.data = []; // Remove node v from gt[i].T; deleting later to preserve tree traversal order
                toDelete.push(node.key);
              }
            });
            plv8.elog(log_level, `final g.tstart: ${g.tstart}, g.tend: ${g.tend}`);
            const final_g_eff = `["${toIsoString(g.tstart)}", "${isNaN(g.tend) ? 'infinity' : toIsoString(g.tend)}")`
            plv8.elog(log_level, `final_g_eff: ${final_g_eff}`);
            plv8.execute(`UPDATE tmda_ci_aggr_group SET effective='${final_g_eff}' WHERE id='${g.id}'`);
            toDelete.forEach(key => tree.remove(key));
          }
          // v ← node in gt[i].T with time v.t = r.Te (insert a new node if required); no duplicates
          let node = tree.insert(row.effective_end, []);

          node = node === null ? tree.find(row.effective_end) : node; // v.open ← v.open ∪ r[A1, . . . , Ap, Ts];
          node.data.push(row);

          // END OF ROW PROCESSING
          // debug: print tree
          plv8.elog(log_level, `tree:\n${tree.toString(function(node) {
            return `${toIsoString(node.key)} [${node.data}]`;
          })}`);
        }
      }
    
      rows = cursor.fetch(5);
    }

    plv8.elog(log_level, "ROWS ITERATION DONE");
    const gt_plan = plv8.prepare(`SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group`);
    const gt_cursor = gt_plan.cursor();
    
    rows = gt_cursor.fetch(5);
    while (rows) {
      // foreach gt[i] ∈ gt do
      for (let i = 0; i < rows.length; i++) {
        const g = rows[i];
        plv8.elog(log_level, `g[${i}]: ${JSON.stringify(g)}`);
        const tree = avltrees[g.id-1];
        //   foreach v ∈ gt[i].T in chronological order do
        //   Create result tuple, add it to z, and close past nodes in gt[i].T ;
        tree.forEach(function(node) { // foreach v ∈ gt[i].T in chronological order
          g.tend = node.key; // gt[i].Te ← v.t;

          let aggrResults = new Array(p_aggr_funcs.length).fill(0); // ResultTuple helpers
          let counts = new Array(p_aggr_funcs.length).fill(0);
          tree.forEach(function(resultmember) { // ResultTuple(gt[i], F, C)
            const nodeRows = resultmember.data;

            for (let noderowCount = 0; noderowCount < nodeRows.length; noderowCount++) {
              const noderow = nodeRows[noderowCount];
              
              for (let k = 0; k < p_aggr_funcs.length; k++) {
                // we need: aggr_func, rowname, property
                const aggr_func = p_aggr_funcs[k];
                let attr_scaling = 1;

                if (v_attr_props.get(p_aggr_target[k]) == 'malleable' || v_attr_props.get(p_aggr_target[k]) == 'atomic') {
                  let rowtend = isNaN(noderow.effective_end.getTime()) ? Date.now() : noderow.effective_end.getTime();
                  plv8.elog(log_level, `rowtend: ${rowtend}`);
                  plv8.elog(log_level, `g.tstart.getTime(): ${g.tstart.getTime()}`);
                  plv8.elog(log_level, `noderow.effective_start.getTime(): ${noderow.effective_start.getTime()}`);
                  attr_scaling = (g.tend.getTime() - g.tstart.getTime()) / (rowtend - noderow.effective_start.getTime());
                }
                plv8.elog(log_level, `p_aggr_target[k]: ${p_aggr_target[k]}`);
                plv8.elog(log_level, `attr_scaling: ${attr_scaling}`);

                if (!(v_attr_props.get(p_aggr_target[k]) == 'atomic' && attr_scaling != 1)) {
                  counts[k]++;
                  const rowval = noderow[p_aggr_target[k]] * attr_scaling;
                
                  if (aggr_func == "avg") {
                    aggrResults[k] += rowval;
                  } else if (aggr_func == "sum") {
                    aggrResults[k] += rowval;
                  } else if (aggr_func == "max") {
                    if (counts[k] === 1) { // First row
                      aggrResults[k] = rowval;
                    } else {
                      if (rowval > aggrResults[k]) {
                        aggrResults[k] = rowval;
                      }
                    }
                  } else if (aggr_func == "min") {
                    if (counts[k] === 1) {
                      aggrResults[k] = rowval;
                    } else {
                      if (rowval < aggrResults[k]) {
                        aggrResults[k] = rowval;
                      }
                    }
                  } else if (aggr_func == "count") {
                      aggrResults[k]++;
                  }
                }
              }
            }
          });
          
          let meaningful = false;
          for (let k = 0; k < p_aggr_funcs.length; k++) {
            if (counts[k] > 0) {
              meaningful = true;
            }

            const aggr_func = p_aggr_funcs[k];
            if (aggr_func == "avg") {
              aggrResults[k] = aggrResults[k] / counts[k];
            }
          }

          if (meaningful) {
            const result_period = `["${toIsoString(g.tstart)}", "${isNaN(g.tend) ? 'infinity' : toIsoString(g.tend)}")`
            let to_return = {};
            for (let k = 0; k < p_aggr_fieldnames.length; k++) {
              const fieldname = p_aggr_fieldnames[k];
              if (counts[k] > 0) {
                to_return[fieldname] = aggrResults[k];
              } else {
                to_return[fieldname] = null;
              }
            }

            for (let k = 0; k < p_group_by.length; k++) {
              const fieldname = p_group_by[k];
              to_return[fieldname] = g[fieldname];
            }

            to_return.effective = result_period;
            plv8.return_next(to_return);
          }

          plv8.elog(log_level, `node.key: ${node.key}`);
          g.tstart = node.key; // gt[i].T ← [v.t + 1, ∗];
          g.tend = new Date(NaN); 
          // plv8.elog(log_level, `g.tstart: ${g.tstart}, g.tend: ${g.tend}`);

          node.data = []; // Remove node v from gt[i].T; deleting later to preserve tree traversal order
        });
      }

      rows = gt_cursor.fetch(5);
    }

    gt_cursor.close();
    cursor.close();
    plan.free();
    plv8.execute('DROP TABLE tmda_ci_aggr_group');
$$
LANGUAGE plv8;



CREATE FUNCTION mwta_now(
  p_schema TEXT,
  p_table TEXT,
  p_group_by TEXT[],
  p_aggr_funcs TEXT[],
  p_aggr_target TEXT[],
  p_aggr_fieldnames TEXT[],
  p_window_size INTERVAL) 
RETURNS SETOF RECORD AS 
$$
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
    const vars = [p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames];
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

    const v_group_by_q = `SELECT column_name, data_type FROM information_schema.columns WHERE 
    table_schema='${p_schema}' AND table_name='${p_table}' AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`;
    if (p_group_by.length > 0) {
      plv8.elog(log_level, v_group_by_q);
      var v_group_by = plv8.execute(v_group_by_q);
      plv8.elog(log_level, `v_group_by: ${v_group_by}`);

      v_group_by.forEach(col => {
        col.column_name = `"${col.column_name}"`
      });
      var v_group_by_cols = v_group_by.map(col => col.column_name);
    }
    let v_aggr_target_cols = p_aggr_target.map(col => `"${col}"`);
    const create_temp_table_q = 
      p_group_by.length == 0 ? 
        `CREATE TEMP TABLE tmda_ci_aggr_group(
        id SERIAL PRIMARY KEY,
        effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
        )`
        : 
        `CREATE TEMP TABLE tmda_ci_aggr_group(
        id SERIAL PRIMARY KEY,
        ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
        effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
        )`;
    plv8.elog(log_level, create_temp_table_q);
    plv8.execute(create_temp_table_q);
    if (p_group_by.length > 0) {
      plv8.execute(`CREATE INDEX lookup_tmda ON tmda_ci_aggr_group (${v_group_by_cols.join(', ')})`);
    }
    const gt_insert_q = 
    p_group_by.length == 0 ? 
    `WITH rows AS (
      INSERT INTO tmda_ci_aggr_group(id) VALUES (1)
      RETURNING 1
    ) SELECT count(*) AS rownum FROM rows`
    : 
    `WITH rows AS (
      INSERT INTO tmda_ci_aggr_group(${v_group_by_cols.join(', ')})
    (
      SELECT DISTINCT
        ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}
      FROM ${`"${p_schema}"."${p_table}"`}
    ) RETURNING 1
    ) SELECT count(*) AS rownum FROM rows`;
    let group_table_count = plv8.execute(gt_insert_q)[0].rownum;
    plv8.elog(log_level, `gt_insert_q: ${gt_insert_q} gt_rowcount: ${group_table_count}`);

    // const debug_gt = plv8.execute(`SELECT * FROM tmda_ci_aggr_group`);
    // for (let i = 0; i < debug_gt.length; i++) {
    //   const g = debug_gt[i];
    //   plv8.elog(log_level, `g[${i}]: ${JSON.stringify(g)}`);
    // }

    plv8.execute('ANALYZE tmda_ci_aggr_group');

    plv8.elog(log_level, "PREP DONE");
    // COMPUTE: Iterate rows and compute result set

    var v_attr_props_query = plv8.execute(`SELECT attr_name, attr_property FROM bitemporal_internal.temporal_attribute_properties
    WHERE schema_name=$1 AND table_name=$2 AND attr_name IN (${p_aggr_target.map(x => `'${x}'`).join(', ')})`,
    [p_schema, p_table]);
    var v_attr_props = new Map();

    v_attr_props_query.forEach(col => v_attr_props.set(col.attr_name, col.attr_property));
    plv8.elog(log_level, `v_attr_props: ${[...v_attr_props.entries()]}`);

    let avltrees = [];
    for (let i = 0; i < group_table_count; i++)
      avltrees.push(new AVLTree(tmdaDateComparator, true));

    const plan_q =
      p_group_by.length == 0 ?
      `SELECT
      ${v_aggr_target_cols.join(", ")},
      LOWER(effective) AS effective_start, UPPER(effective) AS effective_end, (UPPER(effective) + '${p_window_size}'::interval) AS extended_end
      FROM ${`"${p_schema}"."${p_table}"`} WHERE now() <@ asserted
      ORDER BY LOWER(effective)`
      :
      `SELECT 
      ${v_group_by_cols.join(", ")},
      ${v_aggr_target_cols.join(", ")},
      LOWER(effective) AS effective_start, UPPER(effective) AS effective_end, (UPPER(effective) + '${p_window_size}'::interval) AS extended_end
      FROM ${`"${p_schema}"."${p_table}"`} WHERE now() <@ asserted
      ORDER BY LOWER(effective)`;
    const plan = plv8.prepare(plan_q);
    const cursor = plan.cursor();

    let rows = cursor.fetch(5);
    while (rows) { // foreach tuple r ∈ r in chronological order
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const gt_lookup_q = 
        p_group_by.length == 0 ? 
        `SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group`
        :
        `SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group WHERE
        ${v_group_by_cols.map(col => `${col}='${row[col.slice(1, -1)]}'`).join(",\n")}`;
        plv8.elog(log_level, gt_lookup_q);
        const gt_lookup = plv8.execute(gt_lookup_q);

        for (let j = 0; j < gt_lookup.length; j++) { // foreach i ∈ LOOKUP(gt, r, θ)
          const g = gt_lookup[j];
          const tree = avltrees[g.id-1];

          if (isNaN(g.tstart) || row.effective_start > g.tstart) { // Check -infinity or r.ts > gt[i].ts
            // const startminusone = new Date(row.effective_start.getTime()-1); // Insert r.Ts-1
            if (!tree.find(row.effective_start)) tree.insert(row.effective_start, []);

            let toDelete = [];
            tree.forEach(function(node) { // foreach v ∈ gt[i].T in chronological order
              if (node.key <= row.effective_start) { // where v.t < r.Ts
                g.tend = node.key; // gt[i].Te ← v.t;

                let aggrResults = new Array(p_aggr_funcs.length).fill(0); // ResultTuple helpers
                let counts = new Array(p_aggr_funcs.length).fill(0);
                let count = 0;
                tree.forEach(function(resultmember) { // ResultTuple(gt[i], F, C)
                  const nodeRows = resultmember.data;
                  for (let noderowCount = 0; noderowCount < nodeRows.length; noderowCount++) {
                    const noderow = nodeRows[noderowCount];
                    count++;
                    
                    for (let k = 0; k < p_aggr_funcs.length; k++) {
                      // we need: aggr_func, rowname, property
                      const aggr_func = p_aggr_funcs[k];
                      let attr_scaling = 1;

                      if (v_attr_props.get(p_aggr_target[k]) == 'malleable' || v_attr_props.get(p_aggr_target[k]) == 'atomic') {
                        let rowtend = isNaN(noderow.extended_end.getTime()) ? Date.now() : noderow.extended_end.getTime();
                        plv8.elog(log_level, `rowtend: ${rowtend}`);
                        plv8.elog(log_level, `g.tstart.getTime(): ${g.tstart.getTime()}`);
                        plv8.elog(log_level, `noderow.effective_start.getTime(): ${noderow.effective_start.getTime()}`);
                        attr_scaling = (g.tend.getTime() - g.tstart.getTime()) / (rowtend - noderow.effective_start.getTime());
                      }
                      plv8.elog(log_level, `p_aggr_target[k]: ${p_aggr_target[k]}`);
                      plv8.elog(log_level, `attr_scaling: ${attr_scaling}`);

                      if (!(v_attr_props.get(p_aggr_target[k]) == 'atomic' && attr_scaling != 1)) {
                        counts[k]++;
                        const rowval = noderow[p_aggr_target[k]] * attr_scaling;
                      
                        if (aggr_func == "avg") {
                          aggrResults[k] += rowval;
                        } else if (aggr_func == "sum") {
                          aggrResults[k] += rowval;
                        } else if (aggr_func == "max") {
                          if (count === 1) { // First row
                            aggrResults[k] = rowval;
                          } else {
                            if (rowval > aggrResults[k]) {
                              aggrResults[k] = rowval;
                            }
                          }
                        } else if (aggr_func == "min") {
                          if (count === 1) {
                            aggrResults[k] = rowval;
                          } else {
                            if (rowval < aggrResults[k]) {
                              aggrResults[k] = rowval;
                            }
                          }
                        } else if (aggr_func == "count") {
                            aggrResults[k]++;
                        }
                      }
                    }
                  }
                });
                
                let meaningful = false;
                for (let k = 0; k < p_aggr_funcs.length; k++) {
                  if (counts[k] > 0) {
                    meaningful = true;
                  }
      
                  const aggr_func = p_aggr_funcs[k];
                  if (aggr_func == "avg") {
                    aggrResults[k] = aggrResults[k] / counts[k];
                  }
                }
      
                if (meaningful) {
                  const result_period = `["${toIsoString(g.tstart)}", "${isNaN(g.tend) ? 'infinity' : toIsoString(g.tend)}")`
                  let to_return = {};
                  for (let k = 0; k < p_aggr_fieldnames.length; k++) {
                    const fieldname = p_aggr_fieldnames[k];
                    if (counts[k] > 0) {
                      to_return[fieldname] = aggrResults[k];
                    } else {
                      to_return[fieldname] = null;
                    }
                  }
      
                  for (let k = 0; k < p_group_by.length; k++) {
                    const fieldname = p_group_by[k];
                    to_return[fieldname] = g[fieldname];
                  }
      
                  to_return.effective = result_period;
                  plv8.return_next(to_return);
                }

                plv8.elog(log_level, `node.key: ${node.key}`);
                g.tstart = node.key; // gt[i].T ← [v.t + 1, ∗];
                g.tend = new Date(NaN); 
                // plv8.elog(log_level, `g.tstart: ${g.tstart}, g.tend: ${g.tend}`);

                node.data = []; // Remove node v from gt[i].T; deleting later to preserve tree traversal order
                toDelete.push(node.key);
              }
            });
            plv8.elog(log_level, `final g.tstart: ${g.tstart}, g.tend: ${g.tend}`);
            const final_g_eff = `["${toIsoString(g.tstart)}", "${isNaN(g.tend) ? 'infinity' : toIsoString(g.tend)}")`
            plv8.elog(log_level, `final_g_eff: ${final_g_eff}`);
            plv8.execute(`UPDATE tmda_ci_aggr_group SET effective='${final_g_eff}' WHERE id='${g.id}'`);
            toDelete.forEach(key => tree.remove(key));
          }
          // v ← node in gt[i].T with time v.t = r.Te (insert a new node if required); no duplicates
          let node = tree.insert(row.extended_end, []);

          node = node === null ? tree.find(row.extended_end) : node; // v.open ← v.open ∪ r[A1, . . . , Ap, Ts];
          node.data.push(row);

          // END OF ROW PROCESSING
          // debug: print tree
          plv8.elog(log_level, `tree:\n${tree.toString(function(node) {
            return `${toIsoString(node.key)} [${node.data}]`;
          })}`);
        }
      }
    
      rows = cursor.fetch(5);
    }

    plv8.elog(log_level, "ROWS ITERATION DONE");
    const gt_plan = plv8.prepare(`SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group`);
    const gt_cursor = gt_plan.cursor();
    
    rows = gt_cursor.fetch(5);
    while (rows) {
      // foreach gt[i] ∈ gt do
      for (let i = 0; i < rows.length; i++) {
        const g = rows[i];
        plv8.elog(log_level, `g[${i}]: ${JSON.stringify(g)}`);
        const tree = avltrees[g.id-1];
        //   foreach v ∈ gt[i].T in chronological order do
        //   Create result tuple, add it to z, and close past nodes in gt[i].T ;
        tree.forEach(function(node) { // foreach v ∈ gt[i].T in chronological order
          g.tend = node.key; // gt[i].Te ← v.t;

          let aggrResults = new Array(p_aggr_funcs.length).fill(0); // ResultTuple helpers
          let counts = new Array(p_aggr_funcs.length).fill(0);
          let count = 0;
          tree.forEach(function(resultmember) { // ResultTuple(gt[i], F, C)
            const nodeRows = resultmember.data;

            for (let noderowCount = 0; noderowCount < nodeRows.length; noderowCount++) {
              const noderow = nodeRows[noderowCount];
              count++;
              
              for (let k = 0; k < p_aggr_funcs.length; k++) {
                // we need: aggr_func, rowname, property
                const aggr_func = p_aggr_funcs[k];
                let attr_scaling = 1;

                if (v_attr_props.get(p_aggr_target[k]) == 'malleable' || v_attr_props.get(p_aggr_target[k]) == 'atomic') {
                  let rowtend = isNaN(noderow.extended_end.getTime()) ? Date.now() : noderow.extended_end.getTime();
                  plv8.elog(log_level, `rowtend: ${rowtend}`);
                  plv8.elog(log_level, `g.tstart.getTime(): ${g.tstart.getTime()}`);
                  plv8.elog(log_level, `noderow.effective_start.getTime(): ${noderow.effective_start.getTime()}`);
                  attr_scaling = (g.tend.getTime() - g.tstart.getTime()) / (rowtend - noderow.effective_start.getTime());
                }
                plv8.elog(log_level, `p_aggr_target[k]: ${p_aggr_target[k]}`);
                plv8.elog(log_level, `attr_scaling: ${attr_scaling}`);

                if (!(v_attr_props.get(p_aggr_target[k]) == 'atomic' && attr_scaling != 1)) {
                  counts[k]++;
                  const rowval = noderow[p_aggr_target[k]] * attr_scaling;
                
                  if (aggr_func == "avg") {
                    aggrResults[k] += rowval;
                  } else if (aggr_func == "sum") {
                    aggrResults[k] += rowval;
                  } else if (aggr_func == "max") {
                    if (count === 1) { // First row
                      aggrResults[k] = rowval;
                    } else {
                      if (rowval > aggrResults[k]) {
                        aggrResults[k] = rowval;
                      }
                    }
                  } else if (aggr_func == "min") {
                    if (count === 1) {
                      aggrResults[k] = rowval;
                    } else {
                      if (rowval < aggrResults[k]) {
                        aggrResults[k] = rowval;
                      }
                    }
                  } else if (aggr_func == "count") {
                      aggrResults[k]++;
                  }
                }
              }
            }
          });
          
          let meaningful = false;
          for (let k = 0; k < p_aggr_funcs.length; k++) {
            if (counts[k] > 0) {
              meaningful = true;
            }

            const aggr_func = p_aggr_funcs[k];
            if (aggr_func == "avg") {
              aggrResults[k] = aggrResults[k] / counts[k];
            }
          }

          if (meaningful) {
            const result_period = `["${toIsoString(g.tstart)}", "${isNaN(g.tend) ? 'infinity' : toIsoString(g.tend)}")`
            let to_return = {};
            for (let k = 0; k < p_aggr_fieldnames.length; k++) {
              const fieldname = p_aggr_fieldnames[k];
              if (counts[k] > 0) {
                to_return[fieldname] = aggrResults[k];
              } else {
                to_return[fieldname] = null;
              }
            }

            for (let k = 0; k < p_group_by.length; k++) {
              const fieldname = p_group_by[k];
              to_return[fieldname] = g[fieldname];
            }

            to_return.effective = result_period;
            plv8.return_next(to_return);
          }

          plv8.elog(log_level, `node.key: ${node.key}`);
          g.tstart = node.key; // gt[i].T ← [v.t + 1, ∗];
          g.tend = new Date(NaN); 
          // plv8.elog(log_level, `g.tstart: ${g.tstart}, g.tend: ${g.tend}`);

          node.data = []; // Remove node v from gt[i].T; deleting later to preserve tree traversal order
        });
      }

      rows = gt_cursor.fetch(5);
    }

    gt_cursor.close();
    cursor.close();
    plan.free();
    plv8.execute('DROP TABLE tmda_ci_aggr_group');
$$
LANGUAGE plv8;

CREATE FUNCTION tmda_fi_now(
  p_schema TEXT,
  p_table TEXT,
  p_group_by TEXT[],
  p_aggr_funcs TEXT[],
  p_aggr_target TEXT[],
  p_aggr_fieldnames TEXT[],
  p_interval INTERVAL) 
RETURNS SETOF RECORD AS 
$$
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

  // SETUP
  plv8.execute(`SET intervalstyle TO 'postgres_verbose'`);

  const v_group_by_q = `SELECT column_name, data_type FROM information_schema.columns WHERE 
  table_schema='${p_schema}' AND table_name='${p_table}' AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`;
  if (p_group_by.length > 0) {
    plv8.elog(log_level, v_group_by_q);
    var v_group_by = plv8.execute(v_group_by_q);
    plv8.elog(log_level, `v_group_by: ${v_group_by}`);

    v_group_by.forEach(col => {
      col.column_name = `"${col.column_name}"`
    });
    var v_group_by_cols = v_group_by.map(col => col.column_name);
  }
  let v_aggr_target_cols = p_aggr_target.map(col => `"${col}"`);

  const create_temp_table_q = 
  p_group_by.length == 0 ? 
    `CREATE TEMP TABLE tmda_fi_aggr_group(
    id SERIAL PRIMARY KEY,
    effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
    )`
    : 
    `CREATE TEMP TABLE tmda_fi_aggr_group(
    id SERIAL PRIMARY KEY,
    ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
    effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
    )`;
  plv8.elog(log_level, create_temp_table_q);
  plv8.execute(create_temp_table_q);

  if (p_group_by.length == 0) {
    plv8.execute(`CREATE INDEX lookup_tmda ON tmda_fi_aggr_group USING GIST (effective)`);
  } else {
    plv8.execute(`CREATE INDEX lookup_tmda ON tmda_fi_aggr_group USING GIST (${v_group_by_cols.join(', ')}, effective)`);
  }
  const truncdate = p_interval.split(" ")[2];
  plv8.elog(log_level, `p_interval: ${p_interval} split: ${p_interval.split(" ")} truncdate: ${truncdate}`);
  let series_endpoints = plv8.execute(`SELECT date_trunc('${truncdate}', MIN(LOWER(effective)))::text AS start, MAX(UPPER(effective))::text AS end
  FROM ${`"${p_schema}"."${p_table}"`} WHERE LOWER(effective) != '-infinity' AND UPPER(effective) != 'infinity' AND now() <@ asserted`)[0];

  const gt_insert_q =
  p_group_by.length == 0 ?
  `WITH rows AS (
    INSERT INTO tmda_fi_aggr_group(effective)
  (
    SELECT DISTINCT
      temporal_relationships.timeperiod(t.i, (t.i + '${p_interval}')) FROM
    generate_series('${series_endpoints.start}'::timestamptz, '${series_endpoints.end}'::timestamptz, '${p_interval}'::interval) AS t(i)
  ) RETURNING 1
  ) SELECT count(*) AS rownum FROM rows`
  :
  `WITH rows AS (
    INSERT INTO tmda_fi_aggr_group(${v_group_by_cols.join(', ')}, effective)
  (
    SELECT DISTINCT
      ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}, 
      temporal_relationships.timeperiod(t.i, (t.i + '${p_interval}'))
    FROM ${`"${p_schema}"."${p_table}"`} CROSS JOIN 
    generate_series('${series_endpoints.start}'::timestamptz, '${series_endpoints.end}'::timestamptz, '${p_interval}'::interval) AS t(i)
  ) RETURNING 1
  ) SELECT count(*) AS rownum FROM rows`;
  plv8.elog(log_level, `gt_insert_q: ${gt_insert_q}`);
  let group_table_count = plv8.execute(gt_insert_q)[0].rownum;

  plv8.execute('ANALYZE tmda_fi_aggr_group');

  plv8.elog(log_level, "PREP DONE");
  // COMPUTE: Iterate rows and compute result set

  var v_attr_props_query = plv8.execute(`SELECT attr_name, attr_property FROM bitemporal_internal.temporal_attribute_properties
  WHERE schema_name=$1 AND table_name=$2 AND attr_name IN (${p_aggr_target.map(x => `'${x}'`).join(', ')})`,
  [p_schema, p_table]);
  var v_attr_props = new Map();

  v_attr_props_query.forEach(col => v_attr_props.set(col.attr_name, col.attr_property));
  plv8.elog(log_level, `v_attr_props: ${[...v_attr_props.entries()]}`);

  const gt_plan_q = 
  p_group_by.length == 0 ? 
  `SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_fi_aggr_group`
  :
  `SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_fi_aggr_group`;
  plv8.elog(log_level, gt_plan_q);
  const gt_plan = plv8.prepare(gt_plan_q);
  const gtcursor = gt_plan.cursor();

  let gtrows = gtcursor.fetch(5);

  while (gtrows) {
    // select where equal group by AND interval && interval
    for (let i = 0; i < gtrows.length; i++) {
      const gtrow = gtrows[i];

      const targetplan_q =
      p_group_by.length == 0 ?
      `SELECT
      ${v_aggr_target_cols.join(", ")},
      LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
      FROM ${`"${p_schema}"."${p_table}"`} WHERE now() <@ asserted AND effective && '${gtrow.effective}'`
      :
      `SELECT 
      ${v_group_by_cols.join(", ")},
      ${v_aggr_target_cols.join(", ")},
      LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
      FROM ${`"${p_schema}"."${p_table}"`} WHERE now() <@ asserted AND effective && '${gtrow.effective}' AND
      ${v_group_by_cols.map(col => `${col}='${row[col.slice(1, -1)]}'`).join(" AND ")}`;
      const targetplan = plv8.prepare(targetplan_q);
      const targetcursor = targetplan.cursor();
      
      let aggrResults = new Array(p_aggr_funcs.length).fill(0); // ResultTuple helpers
      let counts = new Array(p_aggr_funcs.length).fill(0);
      let targets = targetcursor.fetch(5);
      while (targets) {
        for (let j = 0; j < targets.length; j++) {
          const target = targets[j];
          
          for (let k = 0; k < p_aggr_funcs.length; k++) {
            // we need: aggr_func, rowname, property
            const aggr_func = p_aggr_funcs[k];
            let attr_scaling = 1;

            if (v_attr_props.get(p_aggr_target[k]) == 'malleable' || v_attr_props.get(p_aggr_target[k]) == 'atomic') {
              let rowtend = isNaN(target.effective_end.getTime()) ? Date.now() : target.effective_end.getTime();
              plv8.elog(log_level, `rowtend: ${rowtend}`);
              plv8.elog(log_level, `gtrow.tstart.getTime(): ${gtrow.tstart.getTime()}`);
              plv8.elog(log_level, `target.effective_start.getTime(): ${target.effective_start.getTime()}`);
              attr_scaling = (gtrow.tend.getTime() - gtrow.tstart.getTime()) / (rowtend - target.effective_start.getTime());
            }
            plv8.elog(log_level, `p_aggr_target[k]: ${p_aggr_target[k]}`);
            plv8.elog(log_level, `attr_scaling: ${attr_scaling}`);

            if (!(v_attr_props.get(p_aggr_target[k]) == 'atomic' && attr_scaling != 1)) {
              counts[k]++;
              const rowval = target[p_aggr_target[k]] * attr_scaling;
            
              if (aggr_func == "avg") {
                aggrResults[k] += rowval;
              } else if (aggr_func == "sum") {
                aggrResults[k] += rowval;
              } else if (aggr_func == "max") {
                if (counts[k] === 1) { // First row
                  aggrResults[k] = rowval;
                } else {
                  if (rowval > aggrResults[k]) {
                    aggrResults[k] = rowval;
                  }
                }
              } else if (aggr_func == "min") {
                if (counts[k] === 1) {
                  aggrResults[k] = rowval;
                } else {
                  if (rowval < aggrResults[k]) {
                    aggrResults[k] = rowval;
                  }
                }
              } else if (aggr_func == "count") {
                  aggrResults[k]++;
              }
            }
          }
        }

        targets = targetcursor.fetch(5);
      }
      let meaningful = false;
      for (let k = 0; k < p_aggr_funcs.length; k++) {
        if (counts[k] > 0) {
          meaningful = true;
        }

        const aggr_func = p_aggr_funcs[k];
        if (aggr_func == "avg") {
          aggrResults[k] = aggrResults[k] / counts[k];
        }
      }

      let to_return = gtrow;
      for (let k = 0; k < p_aggr_fieldnames.length; k++) {
        const fieldname = p_aggr_fieldnames[k];
        if (counts[k] > 0) {
          to_return[fieldname] = aggrResults[k];
        } else {
          to_return[fieldname] = p_aggr_funcs[k] == 'count' ? 0 : null;
        }
      }
      // plv8.elog(INFO, JSON.stringify(to_return));
      delete to_return.id;
      delete to_return.tstart;
      delete to_return.tend;

      plv8.return_next(to_return);

      targetcursor.close();
      targetplan.free();
    }

    gtrows = gtcursor.fetch(5);
  }


  gtcursor.close();
  gt_plan.free();
  plv8.execute('DROP TABLE tmda_fi_aggr_group');
$$
LANGUAGE plv8;

CREATE TYPE bitemporal_internal.temporal_attribute_property_enum AS ENUM ('constant', 'malleable', 'atomic');

CREATE TABLE bitemporal_internal.temporal_attribute_properties(
    schema_name TEXT,
    table_name TEXT,
    attr_name TEXT,
    attr_property bitemporal_internal.temporal_attribute_property_enum,
    PRIMARY KEY (schema_name, table_name, attr_name, attr_property)
);

CREATE INDEX lookup_attr_props ON bitemporal_internal.temporal_attribute_properties (schema_name, table_name, attr_name);

CREATE FUNCTION bitemporal_internal.ll_register_temporal_attribute_property(
    p_schema TEXT,
    p_table TEXT,
    p_attr_name TEXT,
    p_attr_property bitemporal_internal.temporal_attribute_property_enum
) RETURNS INTEGER AS
$body$
    DECLARE
    v_rowcount INTEGER;
    BEGIN
    EXECUTE format($i$INSERT INTO bitemporal_internal.temporal_attribute_properties (schema_name, table_name, attr_name, attr_property)
        VALUES (%L, %L, %L, %L)
        $i$, p_schema, p_table, p_attr_name, p_attr_property);
    GET DIAGNOSTICS v_rowcount:=ROW_COUNT; 
    RETURN v_rowcount;
    END;
$body$
LANGUAGE plpgsql;

CREATE TABLE bitemporal_internal.bitemp_retrieval_utils (
    module TEXT UNIQUE PRIMARY KEY,
    source TEXT
);

INSERT INTO bitemporal_internal.bitemp_retrieval_utils (module, source) VALUES ('AVLTree', $$
/**
 * avl v1.5.3
 * Fast AVL tree for Node and browser
 *
 * @author Alexander Milevski <info@w8r.name>
 * @license MIT
 * @preserve
 */

(function (global, factory) {
  typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
  typeof define === 'function' && define.amd ? define(factory) :
  (global = typeof globalThis !== 'undefined' ? globalThis : global || self, global.AVLTree = factory());
}(this, (function () { 'use strict';

  /**
   * Prints tree horizontally
   * @param  {Node}                       root
   * @param  {Function(node:Node):String} [printNode]
   * @return {String}
   */
  function print (root, printNode) {
    if ( printNode === void 0 ) printNode = function (n) { return n.key; };

    var out = [];
    row(root, '', true, function (v) { return out.push(v); }, printNode);
    return out.join('');
  }

  /**
   * Prints level of the tree
   * @param  {Node}                        root
   * @param  {String}                      prefix
   * @param  {Boolean}                     isTail
   * @param  {Function(in:string):void}    out
   * @param  {Function(node:Node):String}  printNode
   */
  function row (root, prefix, isTail, out, printNode) {
    if (root) {
      out(("" + prefix + (isTail ? '└── ' : '├── ') + (printNode(root)) + "\n"));
      var indent = prefix + (isTail ? '    ' : '│   ');
      if (root.left)  { row(root.left,  indent, false, out, printNode); }
      if (root.right) { row(root.right, indent, true,  out, printNode); }
    }
  }

  /**
   * Is the tree balanced (none of the subtrees differ in height by more than 1)
   * @param  {Node}    root
   * @return {Boolean}
   */
  function isBalanced(root) {
    if (root === null) { return true; } // If node is empty then return true

    // Get the height of left and right sub trees
    var lh = height(root.left);
    var rh = height(root.right);

    if (Math.abs(lh - rh) <= 1 &&
        isBalanced(root.left)  &&
        isBalanced(root.right)) { return true; }

    // If we reach here then tree is not height-balanced
    return false;
  }

  /**
   * The function Compute the 'height' of a tree.
   * Height is the number of nodes along the longest path
   * from the root node down to the farthest leaf node.
   *
   * @param  {Node} node
   * @return {Number}
   */
  function height(node) {
    return node ? (1 + Math.max(height(node.left), height(node.right))) : 0;
  }

  function loadRecursive (parent, keys, values, start, end) {
    var size = end - start;
    if (size > 0) {
      var middle = start + Math.floor(size / 2);
      var key    = keys[middle];
      var data   = values[middle];
      var node   = { key: key, data: data, parent: parent };
      node.left    = loadRecursive(node, keys, values, start, middle);
      node.right   = loadRecursive(node, keys, values, middle + 1, end);
      return node;
    }
    return null;
  }

  function markBalance(node) {
    if (node === null) { return 0; }
    var lh = markBalance(node.left);
    var rh = markBalance(node.right);

    node.balanceFactor = lh - rh;
    return Math.max(lh, rh) + 1;
  }

  function sort(keys, values, left, right, compare) {
    if (left >= right) { return; }

    // eslint-disable-next-line no-bitwise
    var pivot = keys[(left + right) >> 1];
    var i = left - 1;
    var j = right + 1;

    // eslint-disable-next-line no-constant-condition
    while (true) {
      do { i++; } while (compare(keys[i], pivot) < 0);
      do { j--; } while (compare(keys[j], pivot) > 0);
      if (i >= j) { break; }

      var tmp = keys[i];
      keys[i] = keys[j];
      keys[j] = tmp;

      tmp = values[i];
      values[i] = values[j];
      values[j] = tmp;
    }

    sort(keys, values,  left,     j, compare);
    sort(keys, values, j + 1, right, compare);
  }

  // function createNode (parent, left, right, height, key, data) {
  //   return { parent, left, right, balanceFactor: height, key, data };
  // }

  /**
   * @typedef {{
   *   parent:        ?Node,
   *   left:          ?Node,
   *   right:         ?Node,
   *   balanceFactor: number,
   *   key:           Key,
   *   data:          Value
   * }} Node
   */

  /**
   * @typedef {*} Key
   */

  /**
   * @typedef {*} Value
   */

  /**
   * Default comparison function
   * @param {Key} a
   * @param {Key} b
   * @returns {number}
   */
  function DEFAULT_COMPARE (a, b) { return a > b ? 1 : a < b ? -1 : 0; }

  /**
   * Single left rotation
   * @param  {Node} node
   * @return {Node}
   */
  function rotateLeft (node) {
    var rightNode = node.right;
    node.right    = rightNode.left;

    if (rightNode.left) { rightNode.left.parent = node; }

    rightNode.parent = node.parent;
    if (rightNode.parent) {
      if (rightNode.parent.left === node) {
        rightNode.parent.left = rightNode;
      } else {
        rightNode.parent.right = rightNode;
      }
    }

    node.parent    = rightNode;
    rightNode.left = node;

    node.balanceFactor += 1;
    if (rightNode.balanceFactor < 0) {
      node.balanceFactor -= rightNode.balanceFactor;
    }

    rightNode.balanceFactor += 1;
    if (node.balanceFactor > 0) {
      rightNode.balanceFactor += node.balanceFactor;
    }
    return rightNode;
  }

  function rotateRight (node) {
    var leftNode = node.left;
    node.left = leftNode.right;
    if (node.left) { node.left.parent = node; }

    leftNode.parent = node.parent;
    if (leftNode.parent) {
      if (leftNode.parent.left === node) {
        leftNode.parent.left = leftNode;
      } else {
        leftNode.parent.right = leftNode;
      }
    }

    node.parent    = leftNode;
    leftNode.right = node;

    node.balanceFactor -= 1;
    if (leftNode.balanceFactor > 0) {
      node.balanceFactor -= leftNode.balanceFactor;
    }

    leftNode.balanceFactor -= 1;
    if (node.balanceFactor < 0) {
      leftNode.balanceFactor += node.balanceFactor;
    }

    return leftNode;
  }

  // function leftBalance (node) {
  //   if (node.left.balanceFactor === -1) rotateLeft(node.left);
  //   return rotateRight(node);
  // }

  // function rightBalance (node) {
  //   if (node.right.balanceFactor === 1) rotateRight(node.right);
  //   return rotateLeft(node);
  // }

  var AVLTree = function AVLTree (comparator, noDuplicates) {
    if ( noDuplicates === void 0 ) noDuplicates = false;

    this._comparator = comparator || DEFAULT_COMPARE;
    this._root = null;
    this._size = 0;
    this._noDuplicates = !!noDuplicates;
  };

  var prototypeAccessors = { size: { configurable: true } };

  /**
   * Clear the tree
   * @return {AVLTree}
   */
  AVLTree.prototype.destroy = function destroy () {
    return this.clear();
  };

  /**
   * Clear the tree
   * @return {AVLTree}
   */
  AVLTree.prototype.clear = function clear () {
    this._root = null;
    this._size = 0;
    return this;
  };

  /**
   * Number of nodes
   * @return {number}
   */
  prototypeAccessors.size.get = function () {
    return this._size;
  };

  /**
   * Whether the tree contains a node with the given key
   * @param{Key} key
   * @return {boolean} true/false
   */
  AVLTree.prototype.contains = function contains (key) {
    if (this._root){
      var node     = this._root;
      var comparator = this._comparator;
      while (node){
        var cmp = comparator(key, node.key);
        if    (cmp === 0) { return true; }
        else if (cmp < 0) { node = node.left; }
        else              { node = node.right; }
      }
    }
    return false;
  };

  /* eslint-disable class-methods-use-this */

  /**
   * Successor node
   * @param{Node} node
   * @return {?Node}
   */
  AVLTree.prototype.next = function next (node) {
    var successor = node;
    if (successor) {
      if (successor.right) {
        successor = successor.right;
        while (successor.left) { successor = successor.left; }
      } else {
        successor = node.parent;
        while (successor && successor.right === node) {
          node = successor; successor = successor.parent;
        }
      }
    }
    return successor;
  };

  /**
   * Predecessor node
   * @param{Node} node
   * @return {?Node}
   */
  AVLTree.prototype.prev = function prev (node) {
    var predecessor = node;
    if (predecessor) {
      if (predecessor.left) {
        predecessor = predecessor.left;
        while (predecessor.right) { predecessor = predecessor.right; }
      } else {
        predecessor = node.parent;
        while (predecessor && predecessor.left === node) {
          node = predecessor;
          predecessor = predecessor.parent;
        }
      }
    }
    return predecessor;
  };
  /* eslint-enable class-methods-use-this */

  /**
   * Callback for forEach
   * @callback forEachCallback
   * @param {Node} node
   * @param {number} index
   */

  /**
   * @param{forEachCallback} callback
   * @return {AVLTree}
   */
  AVLTree.prototype.forEach = function forEach (callback) {
    var current = this._root;
    var s = [], done = false, i = 0;

    while (!done) {
      // Reach the left most Node of the current Node
      if (current) {
        // Place pointer to a tree node on the stack
        // before traversing the node's left subtree
        s.push(current);
        current = current.left;
      } else {
        // BackTrack from the empty subtree and visit the Node
        // at the top of the stack; however, if the stack is
        // empty you are done
        if (s.length > 0) {
          current = s.pop();
          callback(current, i++);

          // We have visited the node and its left
          // subtree. Now, it's right subtree's turn
          current = current.right;
        } else { done = true; }
      }
    }
    return this;
  };

  /**
   * Walk key range from `low` to `high`. Stops if `fn` returns a value.
   * @param{Key}    low
   * @param{Key}    high
   * @param{Function} fn
   * @param{*?}     ctx
   * @return {SplayTree}
   */
  AVLTree.prototype.range = function range (low, high, fn, ctx) {
    var Q = [];
    var compare = this._comparator;
    var node = this._root, cmp;

    while (Q.length !== 0 || node) {
      if (node) {
        Q.push(node);
        node = node.left;
      } else {
        node = Q.pop();
        cmp = compare(node.key, high);
        if (cmp > 0) {
          break;
        } else if (compare(node.key, low) >= 0) {
          if (fn.call(ctx, node)) { return this; } // stop if smth is returned
        }
        node = node.right;
      }
    }
    return this;
  };

  /**
   * Returns all keys in order
   * @return {Array<Key>}
   */
  AVLTree.prototype.keys = function keys () {
    var current = this._root;
    var s = [], r = [], done = false;

    while (!done) {
      if (current) {
        s.push(current);
        current = current.left;
      } else {
        if (s.length > 0) {
          current = s.pop();
          r.push(current.key);
          current = current.right;
        } else { done = true; }
      }
    }
    return r;
  };

  /**
   * Returns `data` fields of all nodes in order.
   * @return {Array<Value>}
   */
  AVLTree.prototype.values = function values () {
    var current = this._root;
    var s = [], r = [], done = false;

    while (!done) {
      if (current) {
        s.push(current);
        current = current.left;
      } else {
        if (s.length > 0) {
          current = s.pop();
          r.push(current.data);
          current = current.right;
        } else { done = true; }
      }
    }
    return r;
  };

  /**
   * Returns node at given index
   * @param{number} index
   * @return {?Node}
   */
  AVLTree.prototype.at = function at (index) {
    // removed after a consideration, more misleading than useful
    // index = index % this.size;
    // if (index < 0) index = this.size - index;

    var current = this._root;
    var s = [], done = false, i = 0;

    while (!done) {
      if (current) {
        s.push(current);
        current = current.left;
      } else {
        if (s.length > 0) {
          current = s.pop();
          if (i === index) { return current; }
          i++;
          current = current.right;
        } else { done = true; }
      }
    }
    return null;
  };

  /**
   * Returns node with the minimum key
   * @return {?Node}
   */
  AVLTree.prototype.minNode = function minNode () {
    var node = this._root;
    if (!node) { return null; }
    while (node.left) { node = node.left; }
    return node;
  };

  /**
   * Returns node with the max key
   * @return {?Node}
   */
  AVLTree.prototype.maxNode = function maxNode () {
    var node = this._root;
    if (!node) { return null; }
    while (node.right) { node = node.right; }
    return node;
  };

  /**
   * Min key
   * @return {?Key}
   */
  AVLTree.prototype.min = function min () {
    var node = this._root;
    if (!node) { return null; }
    while (node.left) { node = node.left; }
    return node.key;
  };

  /**
   * Max key
   * @return {?Key}
   */
  AVLTree.prototype.max = function max () {
    var node = this._root;
    if (!node) { return null; }
    while (node.right) { node = node.right; }
    return node.key;
  };

  /**
   * @return {boolean} true/false
   */
  AVLTree.prototype.isEmpty = function isEmpty () {
    return !this._root;
  };

  /**
   * Removes and returns the node with smallest key
   * @return {?Node}
   */
  AVLTree.prototype.pop = function pop () {
    var node = this._root, returnValue = null;
    if (node) {
      while (node.left) { node = node.left; }
      returnValue = { key: node.key, data: node.data };
      this.remove(node.key);
    }
    return returnValue;
  };

  /**
   * Removes and returns the node with highest key
   * @return {?Node}
   */
  AVLTree.prototype.popMax = function popMax () {
    var node = this._root, returnValue = null;
    if (node) {
      while (node.right) { node = node.right; }
      returnValue = { key: node.key, data: node.data };
      this.remove(node.key);
    }
    return returnValue;
  };

  /**
   * Find node by key
   * @param{Key} key
   * @return {?Node}
   */
  AVLTree.prototype.find = function find (key) {
    var root = this._root;
    // if (root === null)  return null;
    // if (key === root.key) return root;

    var subtree = root, cmp;
    var compare = this._comparator;
    while (subtree) {
      cmp = compare(key, subtree.key);
      if    (cmp === 0) { return subtree; }
      else if (cmp < 0) { subtree = subtree.left; }
      else              { subtree = subtree.right; }
    }

    return null;
  };

  /**
   * Insert a node into the tree
   * @param{Key} key
   * @param{Value} [data]
   * @return {?Node}
   */
  AVLTree.prototype.insert = function insert (key, data) {
    if (!this._root) {
      this._root = {
        parent: null, left: null, right: null, balanceFactor: 0,
        key: key, data: data
      };
      this._size++;
      return this._root;
    }

    var compare = this._comparator;
    var node  = this._root;
    var parent= null;
    var cmp   = 0;

    if (this._noDuplicates) {
      while (node) {
        cmp = compare(key, node.key);
        parent = node;
        if    (cmp === 0) { return null; }
        else if (cmp < 0) { node = node.left; }
        else              { node = node.right; }
      }
    } else {
      while (node) {
        cmp = compare(key, node.key);
        parent = node;
        if    (cmp <= 0){ node = node.left; } //return null;
        else              { node = node.right; }
      }
    }

    var newNode = {
      left: null,
      right: null,
      balanceFactor: 0,
      parent: parent, key: key, data: data
    };
    var newRoot;
    if (cmp <= 0) { parent.left= newNode; }
    else       { parent.right = newNode; }

    while (parent) {
      cmp = compare(parent.key, key);
      if (cmp < 0) { parent.balanceFactor -= 1; }
      else       { parent.balanceFactor += 1; }

      if      (parent.balanceFactor === 0) { break; }
      else if (parent.balanceFactor < -1) {
        // inlined
        //var newRoot = rightBalance(parent);
        if (parent.right.balanceFactor === 1) { rotateRight(parent.right); }
        newRoot = rotateLeft(parent);

        if (parent === this._root) { this._root = newRoot; }
        break;
      } else if (parent.balanceFactor > 1) {
        // inlined
        // var newRoot = leftBalance(parent);
        if (parent.left.balanceFactor === -1) { rotateLeft(parent.left); }
        newRoot = rotateRight(parent);

        if (parent === this._root) { this._root = newRoot; }
        break;
      }
      parent = parent.parent;
    }

    this._size++;
    return newNode;
  };

  /**
   * Removes the node from the tree. If not found, returns null.
   * @param{Key} key
   * @return {?Node}
   */
  AVLTree.prototype.remove = function remove (key) {
    if (!this._root) { return null; }

    var node = this._root;
    var compare = this._comparator;
    var cmp = 0;

    while (node) {
      cmp = compare(key, node.key);
      if    (cmp === 0) { break; }
      else if (cmp < 0) { node = node.left; }
      else              { node = node.right; }
    }
    if (!node) { return null; }

    var returnValue = node.key;
    var max, min;

    if (node.left) {
      max = node.left;

      while (max.left || max.right) {
        while (max.right) { max = max.right; }

        node.key = max.key;
        node.data = max.data;
        if (max.left) {
          node = max;
          max = max.left;
        }
      }

      node.key= max.key;
      node.data = max.data;
      node = max;
    }

    if (node.right) {
      min = node.right;

      while (min.left || min.right) {
        while (min.left) { min = min.left; }

        node.key= min.key;
        node.data = min.data;
        if (min.right) {
          node = min;
          min = min.right;
        }
      }

      node.key= min.key;
      node.data = min.data;
      node = min;
    }

    var parent = node.parent;
    var pp   = node;
    var newRoot;

    while (parent) {
      if (parent.left === pp) { parent.balanceFactor -= 1; }
      else                  { parent.balanceFactor += 1; }

      if      (parent.balanceFactor < -1) {
        // inlined
        //var newRoot = rightBalance(parent);
        if (parent.right.balanceFactor === 1) { rotateRight(parent.right); }
        newRoot = rotateLeft(parent);

        if (parent === this._root) { this._root = newRoot; }
        parent = newRoot;
      } else if (parent.balanceFactor > 1) {
        // inlined
        // var newRoot = leftBalance(parent);
        if (parent.left.balanceFactor === -1) { rotateLeft(parent.left); }
        newRoot = rotateRight(parent);

        if (parent === this._root) { this._root = newRoot; }
        parent = newRoot;
      }

      if (parent.balanceFactor === -1 || parent.balanceFactor === 1) { break; }

      pp   = parent;
      parent = parent.parent;
    }

    if (node.parent) {
      if (node.parent.left === node) { node.parent.left= null; }
      else                         { node.parent.right = null; }
    }

    if (node === this._root) { this._root = null; }

    this._size--;
    return returnValue;
  };

  /**
   * Bulk-load items
   * @param{Array<Key>}keys
   * @param{Array<Value>}[values]
   * @return {AVLTree}
   */
  AVLTree.prototype.load = function load (keys, values, presort) {
      if ( keys === void 0 ) keys = [];
      if ( values === void 0 ) values = [];

    if (this._size !== 0) { throw new Error('bulk-load: tree is not empty'); }
    var size = keys.length;
    if (presort) { sort(keys, values, 0, size - 1, this._comparator); }
    this._root = loadRecursive(null, keys, values, 0, size);
    markBalance(this._root);
    this._size = size;
    return this;
  };

  /**
   * Returns true if the tree is balanced
   * @return {boolean}
   */
  AVLTree.prototype.isBalanced = function isBalanced$1 () {
    return isBalanced(this._root);
  };

  /**
   * String representation of the tree - primitive horizontal print-out
   * @param{Function(Node):string} [printNode]
   * @return {string}
   */
  AVLTree.prototype.toString = function toString (printNode) {
    return print(this._root, printNode);
  };

  Object.defineProperties( AVLTree.prototype, prototypeAccessors );

  AVLTree.default = AVLTree;

  return AVLTree;

})));
//# sourceMappingURL=avl.js.map
$$
);