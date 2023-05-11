\i pg_bitemporal/sql/_load_all.sql

CREATE EXTENSION bitemp_retrieval;

-- Contains
SELECT bitemp_contains_timeslice(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-15'::timestamptz,
    '2020-10-15'::timestamptz
);

-- Outside effective range
SELECT bitemp_contains_timeslice(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-25'::timestamptz,
    '2020-10-15'::timestamptz
);

-- Outside asserted range
SELECT bitemp_contains_timeslice(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-15'::timestamptz,
    '2020-10-25'::timestamptz
);

-- Outside effective & asserted
SELECT bitemp_contains_timeslice(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2021-10-25'::timestamptz,
    '2021-10-25'::timestamptz
);