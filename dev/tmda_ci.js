function tmda_ci_c(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_target, p_aggr_fieldnames) {
    eval(plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'")[0].source);

    var v_group_by = plv8.execute(`SELECT column_name, data_type FROM information_schema.columns WHERE 
    table_schema=$1 AND table_name=$2 AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`,
    [p_schema, p_table]);
    v_group_by.forEach(col => {
      col.column_name = `"${col.column_name}"`
    });
    var v_group_by_cols = v_group_by.map(col => col.column_name);

    plv8.execute(`CREATE TEMP TABLE tmda_ci_aggr_group(
      id SERIAL PRIMARY KEY,
      ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
      effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
      )`);
    plv8.execute(`CREATE INDEX lookup_tmda ON tmda_ci_aggr_group (${v_group_by_cols.join(', ')})`);
    
    var group_table_count = plv8.execute(`WITH rows AS (
      INSERT INTO tmda_ci_aggr_group(${v_group_by_cols.join(', ')})
    (
      SELECT DISTINCT
        ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}
      FROM ${`"${p_schema}"."${p_table}"`}
    ) RETURNING 1
    ) SELECT count(*) AS rownum FROM rows`)[0].rownum;

    plv8.execute('ANALYZE tmda_ci_aggr_group');

    var avltrees = [];
    for (var i = 0; i < group_table_count; i++)
      avltrees.push(new AVLTree());

    plv8.execute('DROP TABLE tmda_ci_aggr_group');
}







function tmda_ci_print(p_schema, p_table, p_group_by, p_aggr_funcs, p_aggr_fieldnames) {
  // eval(plv8.execute("select source from bitemporal_internal.bitemp_retrieval_utils where module = 'AVLTree'")[0].source);

  console.log(`SELECT column_name, data_type FROM information_schema.columns WHERE 
  table_schema=$1 AND table_name=$2 AND column_name IN (${p_group_by.map(x => `'${x}'`).join(', ')})`,
  [p_schema, p_table]);
  var v_group_by = [{"column_name": "d", "data_type":"text"}]
  v_group_by.forEach(col => {
    col.column_name = `"${col.column_name}"`
  });
  var v_group_by_cols = v_group_by.map(col => col.column_name);

  console.log(`CREATE TEMP TABLE tmda_ci_aggr_group(
    id SERIAL PRIMARY KEY,
    ${v_group_by.map(col => `${col.column_name} ${col.data_type}`).join(",\n")},
    effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
    )`);
  console.log(`CREATE INDEX lookup_tmda ON tmda_ci_aggr_group (${v_group_by_cols.join(', ')})`);
  
  var group_table_count = console.log(`WITH rows AS (
    INSERT INTO tmda_ci_aggr_group(${v_group_by_cols.join(', ')})
  (
    SELECT DISTINCT
      ${v_group_by_cols.map(col => `"${p_schema}"."${p_table}".${col}`).join(",\n")}
    FROM ${`"${p_schema}"."${p_table}"`}
  ) RETURNING 1
  ) SELECT count(*) AS rownum FROM rows`);

  console.log('ANALYZE tmda_ci_aggr_group');

  console.log('DROP TABLE tmda_ci_aggr_group');
}

tmda_ci_print('public', 'empl', ['d'], ['sum'], ['h'], ['sum_h']);