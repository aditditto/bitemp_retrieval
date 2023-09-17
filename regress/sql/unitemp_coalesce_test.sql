DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;
SET DATESTYLE TO 'ISO';

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

SELECT * FROM unitemp_coalesce_select(
    'SELECT emp_name, dept, effective FROM employee WHERE now() <@ asserted'::text, 
    ARRAY['emp_name', 'dept'], 
    'effective'::text
    ) AS (emp_name text,
            dept text,
            effective tstzrange) ORDER BY emp_name, effective;

SELECT * FROM unitemp_coalesce_select_effective(
    'SELECT emp_name, dept, effective FROM employee WHERE now() <@ asserted'::text, 
    ARRAY['emp_name', 'dept']
    ) AS (emp_name text,
            dept text,
            effective tstzrange) ORDER BY emp_name, effective;

SELECT * FROM unitemp_coalesce_table_effective(
    'public', 'employee', ARRAY['emp_name', 'dept'])
AS (emp_name text,
        dept text,
        effective tstzrange) ORDER BY emp_name, effective;

SELECT * FROM v8_select_test('dsadsa', '0.5 year'::interval, '{}');

SELECT MAX(UPPER(effective)), date_trunc('years', MAX(UPPER(effective))) FROM employee WHERE UPPER(effective) != 'infinity' AND now() <@ asserted;

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'employee2',
	$$emp_id int, 
	  emp_name text not null,
    dept text not null
	$$,
   'emp_id');

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.employee2',
    $$emp_id, emp_name, dept$$,
    $$1, 'George', 'Ship'$$,
    temporal_relationships.timeperiod('2010-10-08'::timestamptz, '2010-10-11'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.employee2',
    $$emp_id, emp_name, dept$$,
    $$2, 'Ron', 'Ship'$$,
    temporal_relationships.timeperiod('2010-09-28'::timestamptz, '2010-10-01'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
    'public.employee2',
    $$emp_id, emp_name, dept$$,
    $$3, 'Bob', 'Ship'$$,
    temporal_relationships.timeperiod('2010-10-10'::timestamptz, '2010-10-15'::timestamptz), --effective
    temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM unitemp_coalesce_select_effective(
    $$
    SELECT * FROM (
    SELECT emp_name, dept, effective FROM employee WHERE now() <@ asserted
    UNION
    SELECT emp_name, dept, effective FROM employee2 WHERE now() <@ asserted
    ) t
    $$,
    ARRAY['emp_name', 'dept']
) AS (emp_name text,
            dept text,
            effective tstzrange) ORDER BY emp_name, effective;

SELECT * FROM unitemp_coalesce_select(
    $$
    SELECT * FROM (
    SELECT emp_name, dept, effective FROM employee WHERE now() <@ asserted
    UNION
    SELECT emp_name, dept, effective FROM employee2 WHERE now() <@ asserted
    ) t
    $$,
    ARRAY['emp_name', 'dept'],
    'effective'
) AS (emp_name text,
            dept text,
            effective tstzrange) ORDER BY emp_name, effective;


DROP TABLE employee;
DROP TABLE employee2;
DROP TABLE manages;