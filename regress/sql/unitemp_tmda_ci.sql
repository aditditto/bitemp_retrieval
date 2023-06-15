DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;
SET DATESTYLE TO 'ISO';
SET TIMEZONE TO 'Asia/Jakarta';

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'empl',
	$$n text not null, 
        cid int,
        d text not null,
        p text not null,
        h int not null,
        s int not null
	$$,
   'cid');

SELECT * FROM bitemporal_internal.ll_register_temporal_attribute_property('public', 'empl', 'h', 'malleable');

SELECT * FROM bitemporal_internal.temporal_attribute_properties;

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 140, 'DB', 'P1', 2400, 1200$$,
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2004-04-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 163, 'DB', 'P1', 600, 1500$$,
    temporal_relationships.timeperiod('2004-07-01'::timestamptz, '2004-10-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 141, 'DB', 'P2', 500, 700$$,
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2003-06-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 150, 'DB', 'P1', 1000, 800$$,
    temporal_relationships.timeperiod('2003-06-01'::timestamptz, '2004-04-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 157, 'DB', 'P1', 600, 500$$,
    temporal_relationships.timeperiod('2004-01-01'::timestamptz, '2005-01-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Sue', 142, 'DB', 'P2', 400, 800$$,
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2003-11-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Tom', 143, 'AI', 'P2', 1200, 2000$$,
    temporal_relationships.timeperiod('2003-04-01'::timestamptz, '2003-11-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Tom', 153, 'AI', 'P1', 900, 1800$$,
    temporal_relationships.timeperiod('2004-01-01'::timestamptz, '2004-07-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT n, cid, d, p, h, s, effective FROM empl WHERE now() <@ asserted;

PREPARE get_types(text, text) AS 
SELECT column_name, data_type FROM information_schema.columns WHERE 
  table_schema=$1 AND table_name=$2 AND column_name IN ('d');

EXECUTE get_types('public', 'empl');

CREATE TEMP TABLE tmda_ci_aggr_group(
    id SERIAL PRIMARY KEY,
    "d" text,
    effective temporal_relationships.timeperiod DEFAULT '["-infinity", "infinity")'
    );
CREATE INDEX lookup_tmda ON tmda_ci_aggr_group ("d");
WITH rows AS (
    INSERT INTO tmda_ci_aggr_group("d")
  (
    SELECT DISTINCT
      "public"."empl"."d"
    FROM "public"."empl"
  ) RETURNING 1
  ) SELECT count(*) AS rownum FROM rows;

SELECT * FROM tmda_ci_aggr_group;