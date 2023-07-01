import AVLTree from "./avl";

function tmda_ci_c(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames) {
    // PREP: Initialize gt and index
    const log_level = INFO;
    eval(plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'")[0].source);

    // Function for AVL tree insertion, assumes key is range endpoint so NaN is +infinity
    const tmdaDateComparator = function(a, b) {
      if (isNaN(a) && isNaN(b)) return 0;
      if (isNaN(a)) return 1;
      if (isNaN(b)) return -1;

      return a > b ? 1 : a < b ? -1 : 0;
    }
    var v_group_by = plv8.execute(`SELECT column_name, data_type FROM information_schema.columns WHERE 
    table_schema=$1 AND table_name=$2 AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`,
    [p_schema, p_table]);

    v_group_by.forEach(col => {
      col.column_name = `"${col.column_name}"`
    });
    let v_group_by_cols = v_group_by.map(col => col.column_name);
    let v_aggr_target_cols = p_aggr_target.map(col => `"${col}"`);

    plv8.execute(`CREATE TEMP TABLE tmda_ci_aggr_group(
      id SERIAL PRIMARY KEY,
      ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
      effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
      )`);
    plv8.execute(`CREATE INDEX lookup_tmda ON tmda_ci_aggr_group (${v_group_by_cols.join(', ')})`);
    
    let group_table_count = plv8.execute(`WITH rows AS (
      INSERT INTO tmda_ci_aggr_group(${v_group_by_cols.join(', ')})
    (
      SELECT DISTINCT
        ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}
      FROM ${`"${p_schema}"."${p_table}"`}
    ) RETURNING 1
    ) SELECT count(*) AS rownum FROM rows`)[0].rownum;

    plv8.execute('ANALYZE tmda_ci_aggr_group');

    plv8.elog(log_level, "PREP DONE");
    // COMPUTE: Iterate rows and compute result set

    var v_attr_props_query = plv8.execute(`SELECT attr_name, attr_property FROM bitemporal_internal.temporal_attribute_properties
    WHERE schema_name=$1 AND table_name=$2 AND attr_name IN (${p_aggr_target.map(x => `'${x}'`).join(', ')})`,
    [p_schema, p_table]);
    var v_attr_props = new Map();

    v_attr_props_query.forEach(col => v_attr_props.set(col.attr_name, col.attr_property));

    let avltrees = [];
    for (let i = 0; i < group_table_count; i++)
      avltrees.push(new AVLTree(tmdaDateComparator, true));

    const plan = plv8.prepare(`SELECT 
    ${v_group_by_cols.join(", ")},
    ${v_aggr_target_cols.join(", ")},
    LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
    FROM ${`"${p_schema}"."${p_table}"`}
    ORDER BY LOWER(effective)`);
    const cursor = plan.cursor();

    let rows = cursor.fetch(5);
    while (rows.length) { // foreach tuple r ∈ r in chronological order
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const gt_lookup = plv8.execute(`SELECT *, LOWER(effective) AS tstart, UPPER(effective) AS tend FROM tmda_ci_aggr_group WHERE
        ${v_group_by_cols.map(col => `${col}=${row[col.slice(1, -1)]}`).join(",\n")}
        `);

        for (let j = 0; j < gt_lookup.length; j++) { // foreach i ∈ LOOKUP(gt, r, θ)
          const g = gt_lookup[j];
          const tree = avltrees[g.id-1];

          if (isNaN(g.tstart) || row.effective_start > g.tstart) { // Check -infinity or r.ts > gt[i].ts
            const startminusone = new Date(row.effective_start.getTime()-1); // Insert r.Ts-1
            if (!tree.find(startminusone)) tree.insert(startminusone);

            let toDelete = [];
            tree.forEach(function(node) { // foreach v ∈ gt[i].T in chronological order
              if (node.key < row.effective_start) { // where v.t < r.Ts
                g.tend = node.key; // gt[i].Te ← v.t;

                let aggrResults = new Array(p_aggr_funcs.length).fill(0); // ResultTuple helpers
                let count = 0;
                tree.forEach(function(resultmember) { // ResultTuple(gt[i], F, C)
                  let nodeRows = resultmember.data;
                  for (let noderowCount = 0; noderowCount < nodeRows.length; noderowCount++) {
                    const noderow = nodeRows[noderowCount];
                    count++;
                    
                    for (let k = 0; k < p_aggr_funcs.length; k++) {
                      // we need: aggr_func, rowname, property
                      const aggr_func = p_aggr_funcs[k];
                      let attr_scaling = 1;

                      if (v_attr_props.get(p_aggr_target[k]) == 'malleable') {
                        let rowtend = isNaN(noderow.effective_end.getTime()) ? Date.now() : noderow.effective_end.getTime();
                        attr_scaling = (rowtend - g.tstart.getTime()) / (rowtend - noderow.effective_start.getTime());
                      }

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
                });
                
                for (let k = 0; k < p_aggr_funcs.length; k++) {
                  if (aggr_func == "avg") {
                    aggrResults[k] = aggrResults[k] / count;
                  }
                }

                const result_period = `["${g.tstart.toISOString()}", "${isNaN(g.tend) ? 'infinity' : g.tend.toISOString()}")`
                let to_return = {};
                for (let k = 0; k < p_aggr_fieldnames.length; k++) {
                  const fieldname = p_aggr_fieldnames[k];
                  to_return[fieldname] = aggrResults[k];
                }

                for (let k = 0; k < p_group_by.length; k++) {
                  const fieldname = p_group_by[k];
                  to_return[fieldname] = g[fieldname];
                }

                to_return.effective = result_period;
                plv8.return_next(to_return);
                
                g.tstart = node.key; // gt[i].T ← [v.t + 1, ∗];
                g.tend = new Date(NaN); 

                node.data = []; // Remove node v from gt[i].T; deleting later to preserve tree traversal order
                toDelete.push(node.key);
              }
            });

            plv8.execute(`UPDATE tmda_ci_aggr_group SET effective='${final}' WHERE id=${g.id}`);
            toDelete.forEach(key => tree.remove(key));
          }
          // v ← node in gt[i].T with time v.t = r.Te (insert a new node if required); no duplicates
          let node = tree.insert(row.effective_end, []);

          node = node === null ? tree.find(row.effective_end) : node; // v.open ← v.open ∪ r[A1, . . . , Ap, Ts];
          node.data.push(row);
        }
      }
    
      rows = cursor.fetch(5);
    }

    // foreach gt[i] ∈ gt do
    //   foreach v ∈ gt[i].T in chronological order do
    //   Create result tuple, add it to z, and close past nodes in gt[i].T ;
    cursor.close();
    plan.free();
    plv8.execute('DROP TABLE tmda_ci_aggr_group');
}







function tmda_ci_print(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames) {
  // eval(plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'")[0].source);

  console.log(`SELECT column_name, data_type FROM information_schema.columns WHERE 
  table_schema=$1 AND table_name=$2 AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`,
  [p_schema, p_table]);
  var v_group_by = [{"column_name": "d", "data_type":"text"}]
  v_group_by.forEach(col => {
    col.column_name = `"${col.column_name}"`
  });
  var v_group_by_cols = v_group_by.map(col => col.column_name);
  let v_aggr_target_cols = p_aggr_target.map(col => `"${col}"`);

  console.log(`CREATE TEMP TABLE tmda_ci_aggr_group(
    id SERIAL PRIMARY KEY,
    ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
    effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
    )`);
  console.log(`CREATE INDEX lookup_tmda ON tmda_ci_aggr_group (${v_group_by_cols.join(', ')})`);
  
  let group_table_count = console.log(`WITH rows AS (
    INSERT INTO tmda_ci_aggr_group(${v_group_by_cols.join(', ')})
  (
    SELECT DISTINCT
      ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}
    FROM ${`"${p_schema}"."${p_table}"`}
  ) RETURNING 1
  ) SELECT count(*) AS rownum FROM rows`);

  group_table_count = 2;

  console.log('ANALYZE tmda_ci_aggr_group');

  // let avltrees = [];
  // for (let i = 0; i < group_table_count; i++)
  //   avltrees.push(new AVLTree());

  let plan = console.log(`SELECT 
  ${v_group_by_cols.join(", ")},
  ${v_aggr_target_cols.join(", ")},
  LOWER(effective) AS effective_start, UPPER(effective) AS effective_end
  FROM ${`"${p_schema}"."${p_table}"`}
  ORDER BY LOWER(effective)`);

  console.log('DROP TABLE tmda_ci_aggr_group');
}

tmda_ci_print('public', 'empl', ['d'], ['sum', 'max'], ['h', 's'], ['sum_h', 'max_s']);