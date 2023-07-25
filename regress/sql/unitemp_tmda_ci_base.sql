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

-- r1
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 140, 'DB', 'P1', 2400, 1200$$,
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2004-04-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r2
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 163, 'DB', 'P1', 600, 1500$$,
    temporal_relationships.timeperiod('2004-07-01'::timestamptz, '2004-10-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r3
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 141, 'DB', 'P2', 500, 700$$,
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2003-06-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r4
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 150, 'DB', 'P1', 1000, 800$$,
    temporal_relationships.timeperiod('2003-06-01'::timestamptz, '2004-04-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r5
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 157, 'DB', 'P1', 600, 500$$,
    temporal_relationships.timeperiod('2004-01-01'::timestamptz, '2005-01-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r6
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Sue', 142, 'DB', 'P2', 400, 800$$,
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2003-11-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r7
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Tom', 143, 'AI', 'P2', 1200, 2000$$,
    temporal_relationships.timeperiod('2003-04-01'::timestamptz, '2003-11-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- r8
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Tom', 153, 'AI', 'P1', 900, 1800$$,
    temporal_relationships.timeperiod('2004-01-01'::timestamptz, '2004-07-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT n, cid, d, p, h, s, effective FROM empl WHERE now() <@ asserted;

SELECT * FROM tmda_ci(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['sum', 'max'],
    ARRAY['h', 's'],
    ARRAY['sum_h', 'max_s']
) AS (
    d text,
    sum_h numeric,
    max_s numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);


DROP TABLE public.empl;

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

SELECT * FROM tmda_ci(
    'public',
    'empl',
    ARRAY['d'],
    ARRAY['sum', 'max'],
    ARRAY['h', 's'],
    ARRAY['sum_h', 'max_s']
) AS (
    d text,
    sum_h numeric,
    max_s numeric,
    effective tstzrange
) ORDER BY d DESC, lower(effective);

SELECT * FROM tmda_ci(
    'public',
    'empl',
    ARRAY['cid'],
    ARRAY['sum', 'max'],
    ARRAY['h', 's'],
    ARRAY['sum_h', 'max_s']
) AS (
    cid int,
    sum_h numeric,
    max_s numeric,
    effective tstzrange
) ORDER BY LOWER(effective);

DROP TABLE public.empl;