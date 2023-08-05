DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;
SET DATESTYLE TO 'ISO';
SET TIMEZONE TO 'Asia/Jakarta';

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp',
    $$id char(2), 
      name varchar(30) not null,
      salary numeric,
      gender character(1),
      dob date,
      deptname char(30)
    $$,
   'id');

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'skills',
    $$name varchar(30) not null,
      empid char(2)
    $$,
   'name, empid');

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'dept',
    $$name varchar(30) not null,
      budget numeric,
      mgrid char(2)
    $$,
   'name');

select now()::date;

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.dept',
    $$name, budget, mgrid$$,
    $$'Toy', 150, 'DI'$$,
    temporal_relationships.timeperiod('1982-01-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'dept',
    $$budget$$,
    $$200$$,
    'name',
    $$'Toy'$$,
    temporal_relationships.timeperiod('1984-08-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'dept',
    $$budget$$,
    $$100$$,
    'name',
    $$'Toy'$$,
    temporal_relationships.timeperiod('1987-01-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.dept',
    $$name, budget, mgrid$$,
    $$'Book', 50, 'ED'$$,
    temporal_relationships.timeperiod('1987-04-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.emp',
    $$id, name, salary, gender, dob, deptname$$,
    $$'ED', 'Ed', 20, 1, '1955-07-01', 'Toy'$$,
    temporal_relationships.timeperiod('1982-02-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'emp',
    $$salary$$,
    $$30$$,
    'id',
    $$'ED'$$,
    temporal_relationships.timeperiod('1982-06-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'emp',
    $$salary$$,
    $$40$$,
    'id',
    $$'ED'$$,
    temporal_relationships.timeperiod('1985-02-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'emp',
    $$deptname$$,
    $$'Book'$$,
    'id',
    $$'ED'$$,
    temporal_relationships.timeperiod('1987-04-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'emp',
    $$name$$,
    $$'Edward'$$,
    'id',
    $$'ED'$$,
    temporal_relationships.timeperiod('1988-01-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.emp',
    $$id, name, salary, gender, dob, deptname$$,
    $$'DI', 'Di', 30, 0, '1960-10-01', 'Toy'$$,
    temporal_relationships.timeperiod('1982-01-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'emp',
    $$salary$$,
    $$40$$,
    'id',
    $$'DI'$$,
    temporal_relationships.timeperiod('1984-08-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'emp',
    $$salary$$,
    $$50$$,
    'id',
    $$'DI'$$,
    temporal_relationships.timeperiod('1986-09-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT c.relname FROM pg_class c WHERE c.relkind = 'S' order BY c.relname;

\d skills

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.skills',
    $$name, empid$$,
    $$'Typing', 'ED'$$,
    temporal_relationships.timeperiod('1982-04-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.skills',
    $$name, empid$$,
    $$'Filing', 'ED'$$,
    temporal_relationships.timeperiod('1985-01-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.skills',
    $$name, empid$$,
    $$'Driving', 'ED'$$,
    temporal_relationships.timeperiod('1982-01-01'::timestamptz, '1982-05-02'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.skills',
    $$name, empid$$,
    $$'Driving', 'ED'$$,
    temporal_relationships.timeperiod('1984-06-01'::timestamptz, '1988-06-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.skills',
    $$name, empid$$,
    $$'Directing', 'DI'$$,
    temporal_relationships.timeperiod('1982-01-01'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT name, budget, mgrid, effective FROM public.dept WHERE now() <@ asserted;
SELECT id, name, salary, gender, dob, deptname, effective FROM public.emp WHERE now() <@ asserted;
SELECT name, empid, effective FROM public.skills WHERE now() <@ asserted;

SELECT name FROM unitemp_coalesce_select_effective('SELECT name, mgrid, effective FROM dept WHERE now() <@ asserted', ARRAY['name', 'mgrid'])
  AS (
    name VARCHAR(30),
    mgrid CHAR(2),
    effective tstzrange
  ) ORDER BY interval_len(effective) ASC LIMIT 1;

SELECT id, deptname, effective FROM emp WHERE now() <@ asserted;

SELECT * FROM unitemp_coalesce_select_effective('SELECT id, deptname, effective FROM emp WHERE now() <@ asserted', ARRAY['id', 'deptname'])
AS ce1 (
  id CHAR(2),
  deptname VARCHAR(30),
  effective tstzrange
) JOIN unitemp_coalesce_select_effective('SELECT id, deptname, effective FROM emp WHERE now() <@ asserted', ARRAY['id', 'deptname'])
AS ce2 (
  id CHAR(2),
  deptname VARCHAR(30),
  effective tstzrange
) ON ce1.deptname = ce2.deptname
WHERE ce2.id = 'DI' AND ce1.deptname='Book' AND interval_len(ce1.effective) >= interval_len(ce2.effective);

SELECT * FROM unitemp_coalesce_select_effective('SELECT id, deptname, effective FROM emp WHERE now() <@ asserted', ARRAY['id', 'deptname'])
AS ce1 (
  id CHAR(2),
  deptname VARCHAR(30),
  effective tstzrange
) JOIN unitemp_coalesce_select_effective('SELECT id, deptname, effective FROM emp WHERE now() <@ asserted', ARRAY['id', 'deptname'])
AS ce2 (
  id CHAR(2),
  deptname VARCHAR(30),
  effective tstzrange
) ON ce1.deptname = ce2.deptname WHERE TRIM(ce1.deptname) = 'Toy' AND 
ce2.id = 'DI' AND ce1.id != ce2.id AND 
interval_len(ce1.effective) <= interval_len(ce2.effective);

SELECT * FROM unitemp_coalesce_select_effective('SELECT id, salary, effective FROM emp WHERE now() <@ asserted', ARRAY['id', 'salary'])
AS ce1 (
  id CHAR(2),
  salary numeric,
  effective tstzrange
) ORDER BY interval_len(effective) DESC LIMIT 1;

SELECT DISTINCT e1.salary FROM emp e1 JOIN emp e2 ON interval_joinable(e1.effective, e2.effective) AND
  e1.deptname = e2.deptname
  WHERE now() <@ e1.asserted AND now() <@ e2.asserted AND
  e1.id = 'ED' AND e2.id = 'DI';

DROP TABLE public.emp;
DROP TABLE public.dept;
DROP TABLE public.skills;