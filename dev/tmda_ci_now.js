import AVLTree from "./avl";

function tmda_ci_c(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames) {
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
}
