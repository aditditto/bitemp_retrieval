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

-- create schema if not exists test_new_schema_by_extension;