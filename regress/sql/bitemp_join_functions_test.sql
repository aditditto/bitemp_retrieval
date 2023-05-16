DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;

-- 10-----20
-- 10-----20
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

-- 10-----20
-- 10---15
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-15'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-15'::timestamptz)
);

-- 10-----20
--    15--20
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-20'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-20'::timestamptz)
);

-- 10-----20
--  11---19
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-11'::timestamptz, '2020-10-19'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-11'::timestamptz, '2020-10-19'::timestamptz)
);

-- 10---15
-- 10-----20
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-15'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2020-10-10'::timestamptz, '2020-10-15'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

--    15--20
-- 10-----20
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

--  11---19
-- 10-----20
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2020-10-11'::timestamptz, '2020-10-19'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2020-10-11'::timestamptz, '2020-10-19'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

-- 10-----20
--    15-----25
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-25'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-25'::timestamptz)
);

--    15-----25
-- 10-----20
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-25'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

SELECT bitemp_timeperiod_join(
    temporal_relationships.timeperiod('2020-10-15'::timestamptz, '2020-10-25'::timestamptz),
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz)
);

-- Not joinable
SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-11-10'::timestamptz, '2020-11-20'::timestamptz)
);

SELECT bitemp_timeperiod_joinable(
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2020-10-20'::timestamptz),
    temporal_relationships.timeperiod('2020-10-20'::timestamptz, '2020-11-20'::timestamptz)
);