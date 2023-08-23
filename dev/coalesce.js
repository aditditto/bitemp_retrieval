function coalesce_select(p_query, p_list_of_fields, p_temporal_field) {
  function parseISOString(s) {
    var b = s.split(/\D+/);
    return new Date(Date.UTC(b[0], --b[1], b[2], b[3], b[4], b[5], b[6]));
  }

  plv8.execute("SET DATESTYLE TO 'ISO'");
  let fields_string = p_list_of_fields.map((field) => `"${field}"`).join(",");
  let query = `${p_query} ORDER BY ${fields_string}, LOWER("${p_temporal_field}")`;
  if (p_list_of_fields.length == 0)
    query = `${p_query} ORDER BY LOWER("${p_temporal_field}")`;
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
}

let table_query = `SELECT ${fields_string}, \"${p_temporal_field}\" FROM \"${p_schema}\".\"${p_table}\" WHERE now() <@ asserted ORDER BY ${fields_string}, \"${p_temporal_field}\"`;