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