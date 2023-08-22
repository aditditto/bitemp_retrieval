SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp30k',
    $$id integer,
      salary numeric,
      deptid integer
    $$,
'id');

DROP TABLE public.dept;

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'dept',
    $$id integer,
      name TEXT
    $$,
'id');

INSERT INTO public.dept(id, name, effective, asserted)
  SELECT generate_series(0, 100) AS id, 
  md5(random()::text) AS name, 
  tstzrange('1900-01-01'::timestamptz, 'infinity', '[)') AS effective,
  tstzrange(now(), 'infinity', '[)') AS asserted;

INSERT INTO public.emp30k(id, salary, deptid, effective, asserted)
  SELECT id, salary, (id/3)::integer AS deptid, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (
    SELECT generate_series(1, 300, 1) AS id, generate_series(501, 800, 1) AS salary
        ) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-04-10'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp30k;

EXPLAIN ANALYZE SELECT * FROM public.emp30k JOIN public.dept ON public.emp30k.deptid = public.dept.id;

EXPLAIN ANALYZE SELECT public.emp30k.id, public.dept.name, interval_join(public.emp30k.effective, public.dept.effective) AS effective
FROM public.emp30k JOIN public.dept ON 
  public.emp30k.deptid = public.dept.id AND 
  interval_joinable(public.emp30k.effective, public.dept.effective)
  WHERE interval_contains_now(public.emp30k.asserted) AND interval_contains_now(public.dept.asserted);


-- 60k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp60k',
    $$id integer,
      salary numeric,
      deptid integer
    $$,
'id');

INSERT INTO public.emp60k(id, salary, deptid, effective, asserted)
  SELECT id, salary, (id/3)::integer AS deptid, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 300, 1) AS id, generate_series(501, 800, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-07-19'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp60k;

EXPLAIN ANALYZE SELECT * FROM public.emp60k JOIN public.dept ON public.emp60k.deptid = public.dept.id;

EXPLAIN ANALYZE SELECT public.emp60k.id, public.dept.name, interval_join(public.emp60k.effective, public.dept.effective) AS effective
FROM public.emp60k JOIN public.dept ON 
  public.emp60k.deptid = public.dept.id AND 
  interval_joinable(public.emp60k.effective, public.dept.effective)
  WHERE interval_contains_now(public.emp60k.asserted) AND interval_contains_now(public.dept.asserted);


-- 90k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp90k',
    $$id integer,
      salary numeric,
      deptid integer
    $$,
'id');

DROP TABLE public.dept;

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'dept',
    $$id integer,
      name TEXT
    $$,
'id');

INSERT INTO public.dept(id, name, effective, asserted)
  SELECT generate_series(0, 150) AS id, 
  md5(random()::text) AS name, 
  tstzrange('1900-01-01'::timestamptz, 'infinity', '[)') AS effective,
  tstzrange(now(), 'infinity', '[)') AS asserted;

INSERT INTO public.emp90k(id, salary, deptid, effective, asserted)
  SELECT id, salary, (id/3)::integer AS deptid, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 450, 1) AS id, generate_series(501, 950, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-07-19'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp90k;

EXPLAIN ANALYZE SELECT * FROM public.emp90k JOIN public.dept ON public.emp90k.deptid = public.dept.id;

EXPLAIN ANALYZE SELECT public.emp90k.id, public.dept.name, interval_join(public.emp90k.effective, public.dept.effective) AS effective
FROM public.emp90k JOIN public.dept ON 
  public.emp90k.deptid = public.dept.id AND 
  interval_joinable(public.emp90k.effective, public.dept.effective)
  WHERE interval_contains_now(public.emp90k.asserted) AND interval_contains_now(public.dept.asserted);


-- 120k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp120k',
    $$id integer,
      salary numeric,
      deptid integer
    $$,
'id');

DROP TABLE public.dept;

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'dept',
    $$id integer,
      name TEXT
    $$,
'id');

INSERT INTO public.dept(id, name, effective, asserted)
  SELECT generate_series(0, 200) AS id, 
  md5(random()::text) AS name, 
  tstzrange('1900-01-01'::timestamptz, 'infinity', '[)') AS effective,
  tstzrange(now(), 'infinity', '[)') AS asserted;

INSERT INTO public.emp120k(id, salary, deptid, effective, asserted)
  SELECT id, salary, (id/3)::integer AS deptid, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 600, 1) AS id, generate_series(501, 1100, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-07-19'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp120k;

EXPLAIN ANALYZE SELECT * FROM public.emp120k JOIN public.dept ON public.emp120k.deptid = public.dept.id;

EXPLAIN ANALYZE SELECT public.emp120k.id, public.dept.name, interval_join(public.emp120k.effective, public.dept.effective) AS effective
FROM public.emp120k JOIN public.dept ON 
  public.emp120k.deptid = public.dept.id AND 
  interval_joinable(public.emp120k.effective, public.dept.effective)
  WHERE interval_contains_now(public.emp120k.asserted) AND interval_contains_now(public.dept.asserted);


-- 150k



SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp150k',
    $$id integer,
      salary numeric,
      deptid integer
    $$,
'id');

INSERT INTO public.emp150k(id, salary, deptid, effective, asserted)
  SELECT id, salary, (id/3)::integer AS deptid, effective, tstzrange(now(), 'infinity', '[)') AS asserted FROM
  (SELECT generate_series(1, 600, 1) AS id, generate_series(501, 1100, 1) AS salary) AS attrs
  CROSS JOIN (
    SELECT tstzrange(t.d, (t.d + '1 day'::interval), '[)') AS effective FROM
      generate_series('1950-01-01'::timestamptz, '1950-09-07'::timestamptz, '1 day') AS t(d)
      ) eff;

SELECT COUNT(*) FROM public.emp150k;

EXPLAIN ANALYZE SELECT * FROM public.emp150k JOIN public.dept ON public.emp150k.deptid = public.dept.id;

EXPLAIN ANALYZE SELECT public.emp150k.id, public.dept.name, interval_join(public.emp150k.effective, public.dept.effective) AS effective
FROM public.emp150k JOIN public.dept ON 
  public.emp150k.deptid = public.dept.id AND 
  interval_joinable(public.emp150k.effective, public.dept.effective)
  WHERE interval_contains_now(public.emp150k.asserted) AND interval_contains_now(public.dept.asserted);
