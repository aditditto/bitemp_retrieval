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
    temporal_relationships.timeperiod('2020-06-01'::timestamptz, '2020-06-04'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1245', 2$$,
    temporal_relationships.timeperiod('2020-06-03'::timestamptz, '2020-06-06'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1234', 4$$,
    temporal_relationships.timeperiod('2020-06-07'::timestamptz, '2020-06-11'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1245', 2$$,
    temporal_relationships.timeperiod('2020-06-17'::timestamptz, '2020-06-19'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.checkout',
    $$custid, tapenum, cost$$,
    $$'C102', 'T1245', 2$$,
    temporal_relationships.timeperiod('2020-06-19'::timestamptz, '2020-06-21'::timestamptz), --effective
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

SELECT * FROM mwta_now(
    'public',
    'checkout',
    '{}',
    ARRAY['count'],
    ARRAY['custid'],
    ARRAY['count'],
    '2 days'::interval
) AS (
    count numeric,
    effective tstzrange
) ORDER BY lower(effective);

SELECT * FROM tmda_fi_now(
    'public',
    'checkout',
    '{}',
    ARRAY['count'],
    ARRAY['custid'],
    ARRAY['count'],
    '1 week'::interval
) AS (
    count numeric,
    effective tstzrange
);

DROP TABLE checkout;