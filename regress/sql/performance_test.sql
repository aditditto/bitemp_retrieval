SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp30k',
    $$id integer,
      salary numeric
    $$,
'id');

INSERT INTO public.emp30k(id, salary, effective, asserted)
  SELECT id, salary, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 300, 1) AS id, generate_series(501, 800, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-04-10'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp30k;

EXPLAIN ANALYZE SELECT AVG(salary), SUM(salary), MAX(salary), MIN(salary), COUNT(salary)
  FROM public.emp30k;

EXPLAIN ANALYZE SELECT * FROM
  ita_now(
    'public',
    'emp30k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count']
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  mwta_now(
    'public',
    'emp30k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '5 hours'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  sta_now(
    'public',
    'emp30k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '1 week'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );



-- 60k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp60k',
    $$id integer,
      salary numeric
    $$,
'id');

INSERT INTO public.emp60k(id, salary, effective, asserted)
  SELECT id, salary, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 300, 1) AS id, generate_series(501, 800, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-07-19'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp60k;

EXPLAIN ANALYZE SELECT AVG(salary), SUM(salary), MAX(salary), MIN(salary), COUNT(salary)
  FROM public.emp60k;

EXPLAIN ANALYZE SELECT * FROM
  ita_now(
    'public',
    'emp60k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count']
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  mwta_now(
    'public',
    'emp60k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '5 hours'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  sta_now(
    'public',
    'emp60k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '1 week'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );



-- 90k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp90k',
    $$id integer,
      salary numeric
    $$,
'id');

INSERT INTO public.emp90k(id, salary, effective, asserted)
  SELECT id, salary, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 450, 1) AS id, generate_series(501, 950, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-07-19'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp90k;

EXPLAIN ANALYZE SELECT AVG(salary), SUM(salary), MAX(salary), MIN(salary), COUNT(salary)
  FROM public.emp90k;

EXPLAIN ANALYZE SELECT * FROM
  ita_now(
    'public',
    'emp90k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count']
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  mwta_now(
    'public',
    'emp90k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '5 hours'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  sta_now(
    'public',
    'emp90k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '1 week'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );



-- 120k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp120k',
    $$id integer,
      salary numeric
    $$,
'id');

INSERT INTO public.emp120k(id, salary, effective, asserted)
  SELECT id, salary, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 600, 1) AS id, generate_series(501, 1100, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-07-19'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp120k;

EXPLAIN ANALYZE SELECT AVG(salary), SUM(salary), MAX(salary), MIN(salary), COUNT(salary)
  FROM public.emp120k;

EXPLAIN ANALYZE SELECT * FROM
  ita_now(
    'public',
    'emp120k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count']
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  mwta_now(
    'public',
    'emp120k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '5 hours'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  sta_now(
    'public',
    'emp120k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '1 week'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );



-- 150k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp150k',
    $$id integer,
      salary numeric
    $$,
'id');

INSERT INTO public.emp150k(id, salary, effective, asserted)
  SELECT id, salary, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 600, 1) AS id, generate_series(501, 1100, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-09-07'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp150k;

EXPLAIN ANALYZE SELECT AVG(salary), SUM(salary), MAX(salary), MIN(salary), COUNT(salary)
  FROM public.emp150k;

EXPLAIN ANALYZE SELECT * FROM
  ita_now(
    'public',
    'emp150k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count']
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  mwta_now(
    'public',
    'emp150k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '5 hours'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

EXPLAIN ANALYZE SELECT * FROM
  sta_now(
    'public',
    'emp150k',
    '{}',
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    ARRAY['salary', 'salary', 'salary', 'salary', 'salary'],
    ARRAY['avg', 'sum', 'max', 'min', 'count'],
    '1 week'::interval
  ) AS (
    avg numeric,
    sum numeric,
    max numeric,
    min numeric,
    count numeric,
    effective tstzrange
  );

  DROP TABLE emp30k;
  DROP TABLE emp60k;
  DROP TABLE emp90k;
  DROP TABLE emp120k;
  DROP TABLE emp150k;