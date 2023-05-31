commands
```
pg_lsclusters
make
make install
make uninstall
make clean
```

type cast
```
ta_sandbox=# select '2020-10-11'::timestamptz;
      timestamptz       
------------------------
 2020-10-11 00:00:00+07
(1 row)
```
## SPI Functions
Some SPI functions that will be useful for me:
- SPI_execute
- SPI_execute_extended (for custom DestReceiver)
- SPI_prepare
- SPI_cursor_open

Some links:
- https://www.postgresql.org/docs/14/spi.html
- https://postgrespro.com/list/thread-id/2454902
- https://postgrespro.com/list/thread-id/2580555
- https://www.postgresql.org/docs/current/error-message-reporting.html
- https://wiki.postgresql.org/wiki/So,_you_want_to_be_a_developer%3F
- https://www.postgresql.org/docs/8.4/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
- https://stackoverflow.com/questions/23929707/return-dynamic-table-with-unknown-columns-from-pl-pgsql-function

Need to explore more examples in source code
## Temporal coalescing
Best performance seems to be using C. SQL is slower and kind of annoying for multiple explicit columns
### Possible solutions
- Return table
- Modify original table
- Create new table

#### Modify original/Destructive/non idempotent
```
R(S,E, c)
S, E = interval
c = explicit attribute
Select order by c, S

to_merge = get_next_tuple()

for each tuple / while not end of table
      next = get_next_tuple()
      if to_merge.c = next.c AND to_merge.E = next.S:
            to_merge.E = next.E
            next.delete()
      else # to_merge = maximum interval for equivalent c
            to_merge = next
      next = get_next_tuple()
```

#### Create new table
```
R(S,E, c)
S, E = interval
c = explicit attribute
Select order by c, S

to_merge = get_next_tuple()

for each tuple / while not end of table
      next = get_next_tuple()
      if to_merge.c = next.c AND to_merge.E = next.S:
            to_merge.E = next.E
      else # to_merge = maximum interval for equivalent c
            insert(new_table, to_merge)
            to_merge = next
      next = get_next_tuple()
```

#### SPI loop then insert
- Loop with SPI
- Return list of tuple ID and new temporal interval endpoint
- Do whatever with it (insert, etc)
- Kind of bad, memory concerns etc.

#### Coalesce SQL query
```
coalesce_select("SELECT * FROM table")
```

#### Coalesce then query
```
coalesce_on_columns(table_type, columns)
```

```
while defined curr_row
      if curr_row == first_row then
            to_coalesce = curr_row
            new_endpoint = curr_row.timerange.endpoint
      else
            if curr_row == to_coalesce then
                  new_endpoint = curr_row.timerange.endpoint
            else
                  to_coalesce.timerange.endpoint = new_endpoint
                  return_next(to_coalesce)

                  to_coalesce = curr_row
                  new_endpoint = curr_row.timerange.endpoint
end while
to_coalesce.timerange.endpoint = new_endpoint
return_next(to_coalesce)
```