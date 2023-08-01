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

-- r1
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 140, 'DB', 'P1', 2400, 1200$$,
    temporal_relationships.timeperiod('1970-01-01'::timestamptz, '1970-01-16'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r2
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 163, 'DB', 'P1', 600, 1500$$,
    temporal_relationships.timeperiod('1970-01-19'::timestamptz, '1970-01-22'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r3
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 141, 'DB', 'P2', 500, 700$$,
    temporal_relationships.timeperiod('1970-01-01'::timestamptz, '1970-01-06'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r4
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 150, 'DB', 'P1', 1000, 800$$,
    temporal_relationships.timeperiod('1970-01-06'::timestamptz, '1970-01-16'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r5
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 157, 'DB', 'P1', 600, 500$$,
    temporal_relationships.timeperiod('1970-01-13'::timestamptz, '1970-01-25'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r6
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Sue', 142, 'DB', 'P2', 400, 800$$,
    temporal_relationships.timeperiod('1970-01-01'::timestamptz, '1970-01-11'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r7
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Tom', 143, 'AI', 'P2', 1200, 2000$$,
    temporal_relationships.timeperiod('1970-01-04'::timestamptz, '1970-01-11'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r8
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Tom', 153, 'AI', 'P1', 900, 1800$$,
    temporal_relationships.timeperiod('1970-01-13'::timestamptz, '1970-01-19'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT n, cid, d, p, h, s, effective FROM empl WHERE now() <@ asserted ORDER BY LOWER(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['avg'],
    ARRAY['h'],
    ARRAY['avg_h']
) AS (
    d text,
    avg_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['sum'],
    ARRAY['h'],
    ARRAY['sum_h']
) AS (
    d text,
    sum_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['max'],
    ARRAY['h'],
    ARRAY['max_h']
) AS (
    d text,
    max_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['min'],
    ARRAY['h'],
    ARRAY['min_h']
) AS (
    d text,
    min_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['count'],
    ARRAY['h'],
    ARRAY['count_h']
) AS (
    d text,
    count_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM bitemporal_internal.ll_register_temporal_attribute_property('public', 'empl', 'h', 'malleable');

SELECT * FROM bitemporal_internal.temporal_attribute_properties;

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['avg'],
    ARRAY['h'],
    ARRAY['avg_h']
) AS (
    d text,
    avg_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['sum'],
    ARRAY['h'],
    ARRAY['sum_h']
) AS (
    d text,
    sum_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['max'],
    ARRAY['h'],
    ARRAY['max_h']
) AS (
    d text,
    max_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['min'],
    ARRAY['h'],
    ARRAY['min_h']
) AS (
    d text,
    min_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['count'],
    ARRAY['h'],
    ARRAY['count_h']
) AS (
    d text,
    count_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

DELETE FROM bitemporal_internal.temporal_attribute_properties WHERE attr_name = 'h';
SELECT * FROM bitemporal_internal.temporal_attribute_properties;

SELECT * FROM bitemporal_internal.ll_register_temporal_attribute_property('public', 'empl', 'h', 'atomic');

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['avg'],
    ARRAY['h'],
    ARRAY['avg_h']
) AS (
    d text,
    avg_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['sum'],
    ARRAY['h'],
    ARRAY['sum_h']
) AS (
    d text,
    sum_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['max'],
    ARRAY['h'],
    ARRAY['max_h']
) AS (
    d text,
    max_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['min'],
    ARRAY['h'],
    ARRAY['min_h']
) AS (
    d text,
    min_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci_now(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['count'],
    ARRAY['h'],
    ARRAY['count_h']
) AS (
    d text,
    count_h numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);