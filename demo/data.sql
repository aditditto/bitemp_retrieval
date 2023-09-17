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

SELECT * FROM public.dept;
SELECT * FROM public.emp;
SELECT * FROM public.skills;

SELECT name, budget, mgrid, effective FROM public.dept WHERE now() <@ asserted;
SELECT id, name, salary, gender, dob, deptname, effective FROM public.emp WHERE now() <@ asserted;
SELECT name, empid, effective FROM public.skills WHERE now() <@ asserted;