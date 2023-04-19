--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitemp_retrieval" to load this file. \quit

create or replace
FUNCTION get_sum(int, int) RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT
SET search_path = 'temporal_relationships';