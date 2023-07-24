DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;

-- Contains
SELECT bitemp_contains_ts(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-15'::timestamptz,
    '2020-10-15'::timestamptz
);

-- Outside effective range
SELECT bitemp_contains_ts(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-25'::timestamptz,
    '2020-10-15'::timestamptz
);

-- Outside asserted range
SELECT bitemp_contains_ts(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-15'::timestamptz,
    '2020-10-25'::timestamptz
);

SELECT bitemp_contains_ts(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2021-10-25'::timestamptz,
    '2021-10-25'::timestamptz
);

-- Contains
SELECT bitemp_contains_now(
    temporal_relationships.timeperiod('-infinity', 'infinity'),
    temporal_relationships.timeperiod('-infinity', 'infinity')
);

-- Outside effective range
SELECT bitemp_contains_now(
    temporal_relationships.timeperiod('-infinity', '1850-10-20'::timestamptz),
    temporal_relationships.timeperiod('-infinity', 'infinity')
);

-- Outside asserted range
SELECT bitemp_contains_now(
    temporal_relationships.timeperiod('-infinity', 'infinity'),
    temporal_relationships.timeperiod('-infinity', '1850-10-20'::timestamptz)
);

-- Outside effective & asserted
SELECT bitemp_contains_now(
    temporal_relationships.timeperiod('-infinity', '1850-10-20'::timestamptz),
    temporal_relationships.timeperiod('-infinity', '1850-10-20'::timestamptz)
);

SELECT interval_contains_ts(
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-15'::timestamptz
);

-- Outside range
SELECT interval_contains_ts(
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-25'::timestamptz
);

-- Outside range
SELECT interval_contains_ts(
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz),
    '2020-10-20'::timestamptz
);

SELECT interval_contains_now(
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

-- Outside range
SELECT interval_contains_now(
    temporal_relationships.timeperiod('-infinity', 'infinity')
);
