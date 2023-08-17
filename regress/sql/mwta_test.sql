DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;
SET DATESTYLE TO 'ISO';

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'checkout',
    $$custid varchar(4) not null, 
      tapenum varchar(5) not null,
      cost integer
    $$,
   'tapenum');

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C101', 'T1234', 4$$,
    temporal_relationships.timeperiod('1970-01-01'::timestamptz, '1970-01-04'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1245', 2$$,
    temporal_relationships.timeperiod('1970-01-03'::timestamptz, '1970-01-06'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1234', 4$$,
    temporal_relationships.timeperiod('1970-01-07'::timestamptz, '1970-01-11'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1245', 2$$,
    temporal_relationships.timeperiod('1970-01-17'::timestamptz, '1970-01-19'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1245', 2$$,
    temporal_relationships.timeperiod('1970-01-19'::timestamptz, '1970-01-21'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT custid, tapenum, cost, effective FROM checkout;

SELECT * FROM tmda_ci_now(
    'public',
    'checkout',
    '{}',
    ARRAY['count'],
    ARRAY['custid'],
    ARRAY['count']
) AS (
    count numeric,
    effective tstzrange
) ORDER BY lower(effective);

DROP TABLE checkout;