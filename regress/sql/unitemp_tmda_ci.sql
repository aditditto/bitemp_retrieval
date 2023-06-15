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
    temporal_relationships.timeperiod('2003-01-01'::timestamptz, '2004-03-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Jan', 163, 'DB', 'P1', 600, 1500$$,
    temporal_relationships.timeperiod('2004-07-01'::timestamptz, '2004-09-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.empl',
    $$n, cid, d, p, h, s$$,
    $$'Ann', 141, 'DB', 'P2', 500, 1500$$,
    temporal_relationships.timeperiod('2004-07-01'::timestamptz, '2004-09-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT n, cid, d, p, h, s, effective, asserted FROM empl WHERE now() <@ asserted;