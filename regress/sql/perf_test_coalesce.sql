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


EXPLAIN ANALYZE SELECT * FROM public.emp30k WHERE now() <@ asserted;

EXPLAIN ANALYZE SELECT * FROM unitemp_coalesce_table_effective(
    'public', 'emp30k', ARRAY['id', 'salary']
) AS (id numeric, salary numeric, effective tstzrange);


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


EXPLAIN ANALYZE SELECT * FROM public.emp60k WHERE now() <@ asserted;

EXPLAIN ANALYZE SELECT * FROM unitemp_coalesce_table_effective(
    'public', 'emp60k', ARRAY['id', 'salary']
) AS (id numeric, salary numeric, effective tstzrange);


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


EXPLAIN ANALYZE SELECT * FROM public.emp90k WHERE now() <@ asserted;

EXPLAIN ANALYZE SELECT * FROM unitemp_coalesce_table_effective(
    'public', 'emp90k', ARRAY['id', 'salary']
) AS (id numeric, salary numeric, effective tstzrange);


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


EXPLAIN ANALYZE SELECT * FROM public.emp120k WHERE now() <@ asserted;

EXPLAIN ANALYZE SELECT * FROM unitemp_coalesce_table_effective(
    'public', 'emp120k', ARRAY['id', 'salary']
) AS (id numeric, salary numeric, effective tstzrange);


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


EXPLAIN ANALYZE SELECT * FROM public.emp150k WHERE now() <@ asserted;

EXPLAIN ANALYZE SELECT * FROM unitemp_coalesce_table_effective(
    'public', 'emp150k', ARRAY['id', 'salary']
) AS (id numeric, salary numeric, effective tstzrange);