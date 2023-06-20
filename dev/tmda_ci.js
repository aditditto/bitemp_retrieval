import AVLTree from "./avl";

function tmda_ci_c(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames) {
    eval(plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'")[0].source);

    let v_group_by = plv8.execute(`SELECT c.column_name, c.data_type, attr_property FROM 
    information_schema.columns c LEFT JOIN bitemporal_internal.temporal_attribute_properties p
    ON c.table_schema=p.schema_name AND c.table_name=p.table_name AND c.column_name=p.attr_name WHERE 
    c.table_schema=$1 AND c.table_name=$2 AND c.column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`,
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

    let avltrees = [];
    for (let i = 0; i < group_table_count; i++)
      avltrees.push(new AVLTree(null, true));

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
            tree.forEach(function(node) { // foreach v ∈ gt[i].T
              if (node.key < row.effective_start) {
                g.tend = node.key;

                let aggrResults = new Array(p_aggr_funcs.length).fill(0);
                let count = 0;
                tree.forEach(function(resultmember) { // ResultTuple(gt[i], F, C)
                  for (let k = 0; k < p_aggr_funcs.length; k++) {
                    const aggr_func = p_aggr_funcs[k];
                    const rowval = row[p_aggr_target[k]];

                  }
                });
                
                node.data = [];
                toDelete.push(node.key);
              }
            });
            
            plv8.execute(`UPDATE g[i] timerange`);
            toDelete.forEach(key => tree.remove(key));
          }

        }
      }  
    
      rows = cursor.fetch(5);
    }

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