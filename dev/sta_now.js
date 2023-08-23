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
      ${v_group_by_cols.map(col => `${col}='${gtrow[col.slice(1, -1)]}'`).join(" AND ")}`;
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
              attr_scaling = Math.min(1, attr_scaling);
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
}
