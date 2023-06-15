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
    temporal_relationships.timeperiod('2010-10-06'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);


SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'employee',
    $$emp_name, dept$$,
    $$'Ron', 'Mail'$$,
    'emp_id',
    $$1$$,
    temporal_relationships.timeperiod('2010-10-08'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'employee',
    $$emp_name, dept$$,
    $$'Ron', 'Ship'$$,
    'emp_id',
    $$1$$,
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
    'public',
    'employee',
    $$emp_name, dept$$,
    $$'Ron', 'Ship'$$,
    'emp_id',
    $$1$$,
    temporal_relationships.timeperiod('2010-10-15'::timestamptz, 'infinity'), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);


SELECT emp_id, emp_name, dept, effective FROM employee;

SELECT emp_id, emp_name, dept, effective FROM employee WHERE now() <@ asserted ORDER BY emp_name, asserted;

SELECT * FROM unitemp_coalesce_select('SELECT emp_name, dept, effective FROM employee WHERE now() <@ asserted'::text, 
    ARRAY['emp_name', 'dept'], 
    'effective'::text
    ) AS (emp_name text,
            dept text,
            effective tstzrange) ORDER BY emp_name, effective;

DROP TABLE employee;
DROP TABLE manages;