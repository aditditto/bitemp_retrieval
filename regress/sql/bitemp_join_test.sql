DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'employee',
	$$emp_id int, 
	  emp_name text not null,
    dept text not null
	$$,
   'emp_id');

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'manages',
	$$mgr_id int, 
	  mgr_name text not null,
    dept text not null
	$$,
   'mgr_id');

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.employee',
    $$emp_id, emp_name, dept$$,
    $$1, 'Ron', 'Ship'$$,
    temporal_relationships.timeperiod('2010-10-01'::timestamptz, '2010-10-06'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.employee',
    $$emp_id, emp_name, dept$$,
    $$2, 'George', 'Ship'$$,
    temporal_relationships.timeperiod('2010-10-05'::timestamptz, '2010-10-09'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.employee',
    $$emp_id, emp_name, dept$$,
    $$1, 'Ron', 'Mail'$$,
    temporal_relationships.timeperiod('2010-10-06'::timestamptz, '2010-10-11'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.manages',
    $$mgr_id, mgr_name, dept$$,
    $$1, 'Ed', 'Load'$$,
    temporal_relationships.timeperiod('2010-10-03'::timestamptz, '2010-10-09'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.manages',
    $$mgr_id, mgr_name, dept$$,
    $$2, 'Jim', 'Ship'$$,
    temporal_relationships.timeperiod('2010-10-07'::timestamptz, '2010-10-16'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT emp_id, emp_name, dept, effective FROM employee;
SELECT mgr_id, mgr_name, dept, effective FROM manages;

SELECT emp_name, e.dept, m.dept, mgr_name, bitemp_join(e.effective, m.effective) FROM
employee e join manages m ON bitemp_joinable(e.effective, m.effective);

SELECT emp_name, e.dept, m.dept, mgr_name, bitemp_join(e.effective, m.effective) FROM
employee e join manages m ON bitemp_joinable(e.effective, m.effective) AND e.dept = m.dept;

DROP TABLE employee;
DROP TABLE manages;