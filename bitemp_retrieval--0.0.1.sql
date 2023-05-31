--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitemp_retrieval" to load this file. \quit

CREATE OR REPLACE
FUNCTION get_sum(int, int) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT
SET search_path = 'temporal_relationships';

CREATE OR REPLACE
FUNCTION bitemp_contains_timeslice(
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
FUNCTION bitemp_timeperiod_joinable(
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
FUNCTION bitemp_timeperiod_join(
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
    my ($p_query, $p_list_of_fields_ref, $p_temporal_field) = @_;

    my @list_of_fields = @{$p_list_of_fields_ref};
    my $fields_string = join(",", map({ "\"$_\"" } @list_of_fields));

    my $query = "$p_query ORDER BY $fields_string, \"$p_temporal_field\"";
    my $sth = spi_query($query);

    my $curr_row;
    my $to_coalesce = undef;
    my $count = 0;
    my $coalesce_start = undef;
    my $coalesce_end = undef;
    my @endpoints = undef;

    while (defined ($curr_row = spi_fetchrow($sth))) {
        # elog(NOTICE, "curr_row = $curr_row to_coalesce  = $to_coalesce");
        my @endpoints = split(',',substr($curr_row->{$p_temporal_field}, 1, -1));

        if(!(defined $to_coalesce)) {
            # elog(NOTICE, "HELLO HELLO FIRST LOOP");
            $to_coalesce = $curr_row;
            # elog(NOTICE, "to_coalesce  = $to_coalesce");
            $coalesce_start = $endpoints[0];
            $coalesce_end = $endpoints[1];
        } else {
            my $equal = 1;
            ISEQ: foreach my $field (@list_of_fields) {
                if ($curr_row->{$field} ne $to_coalesce->{$field}) {
                    $equal = 0;
                    last ISEQ;
                }
            }

            if ($equal && ($coalesce_end eq $endpoints[0])) {
                $coalesce_end = $endpoints[1];
            } else {
                $to_coalesce->{$p_temporal_field} = "[$coalesce_start,$coalesce_end)";
                return_next($to_coalesce);

                $to_coalesce = $curr_row;
                $coalesce_start = $endpoints[0];
                $coalesce_end = $endpoints[1];
            }
        }
    }

    $to_coalesce->{$p_temporal_field} = "[$coalesce_start,$coalesce_end)";
    return_next($to_coalesce);

    return undef;
$body$
LANGUAGE plperl;

-- CREATE OR REPLACE
-- FUNCTION bitemp_coalesce_in_place(
--     p_schema TEXT,
--     p_table TEXT, -- Name of table created using ll_create_bitemporal_table
--     p_list_of_fields TEXT -- List of explicit fields/nontemporal fields
--     )
-- RETURNS void
-- AS
-- $BODY$
--     DECLARE
--     v_serial_key TEXT;
--     current_serial_key INTEGER;
--     new_effective_endpoint timestamptz;
--     new_asserted_endpoint timestamptz;
--     curr_tup RECORD;
--     prev_tup RECORD;
--     v_fields_array TEXT[];
--     v_current_field TEXT;

--     BEGIN
--     v_serial_key :=p_table||'_key';
--     current_serial_key :=-1;
--     v_fields_array :=string_to_array(p_list_of_fields, ',');

--     FOR i IN 1..array_length(v_fields_array, 1)
--     LOOP
--         v_fields_array[i] := BTRIM(v_fields_array[i]);
--         RAISE NOTICE 'Value: %', v_fields_array[i];
--     END LOOP;

--     -- FOR curr_tup IN EXECUTE format($sel$
--     -- SELECT * FROM %s.%s ORDER BY %, effective, asserted
--     -- $sel$,
--     -- p_schema,
--     -- p_table,
--     -- p_list_of_fields) LOOP

--     -- END LOOP;
--     END;
-- $BODY$
-- LANGUAGE plpgsql;

-- CREATE OR REPLACE
-- FUNCTION unitemp_coalesce_select(
--     p_query TEXT,
--     p_list_of_fields TEXT[],
--     p_temporal_field TEXT
-- )
-- RETURNS BOOLEAN
-- AS
-- $body$
--     DECLARE
--     curr_tup RECORD;
--     prev_tup RECORD DEFAULT NULL;
--     curr_field TEXT;
--     field_match BOOLEAN;
--     tups_match BOOLEAN;
--     BEGIN
--     RETURN tups_match;
--     FOR curr_tup IN EXECUTE(p_query) LOOP
--         tups_match := 't';
--         IF prev_tup IS NULL THEN
--             prev_tup := curr_tup;
--         ELSE
--             FOREACH curr_field IN ARRAY p_list_of_fields LOOP
--                 EXECUTE(format($sel$
--                     SELECT curr_tup.%s = prev_tup.%s
--                     $sel$, 
--                     curr_field, curr_field)
--                 ) INTO field_match;
--                 IF NOT field_match THEN
--                     tups_match := 'f';
--                     EXIT;
--                 END IF;
--             END LOOP;
            
--             IF NOT tups_match THEN
--                 prev_tup.
--             END IF;
--             prev_tup := curr_tup;
--         END IF;
--     END LOOP;
--     END;
-- $body$
-- LANGUAGE plpgsql;
