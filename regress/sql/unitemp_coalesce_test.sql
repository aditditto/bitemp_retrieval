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

-- CREATE TABLE test (
--     i int,
--     v varchar
-- );

-- INSERT INTO test (i, v) VALUES (1, 'first line');
-- INSERT INTO test (i, v) VALUES (2, 'second line');
-- INSERT INTO test (i, v) VALUES (3, 'third line');
-- INSERT INTO test (i, v) VALUES (4, 'immortal');
-- CREATE EXTENSION plperl;
-- CREATE OR REPLACE FUNCTION test_munge() RETURNS SETOF test AS $$
--     my $rv = spi_exec_query('select i, v from test;');
--     my $status = $rv->{status};
--     my $nrows = $rv->{processed};
--     foreach my $rn (0 .. $nrows - 1) {
--         my $row = $rv->{rows}[$rn];
--         $row->{i} += 200 if defined($row->{i});
--         elog(NOTICE, $row->{i});
--         $row->{v} =~ tr/A-Za-z/a-zA-Z/ if (defined($row->{v}));
--         return_next($row);
--     }
--     return undef;
-- $$ LANGUAGE plperl;

-- SELECT * FROM test_munge();

-- SELECT * FROM test;

-- CREATE OR REPLACE FUNCTION test_print() RETURNS void AS $$
--     my $plan = spi_prepare('select * from $1;', 'TEXT');
--     my $rv = spi_exec_prepared($plan, "employee");
--     my $status = $rv->{status};
--     my $nrows = $rv->{processed};

    
--     foreach my $rn (0 .. $nrows - 1) {
--         my %row = %{ $rv->{rows}[$rn] };
--         my $rowstr = '';
--         keys %row;
--         while(my($k, $v) = each %row) { 
--             $rowstr = $rowstr . ' ' . $k . ' ' . $v;
--         }
--         elog(NOTICE, $rowstr);
--     }
--     return undef;
-- $$ LANGUAGE plperl;

-- SELECT test_print();
-- SELECT emp_name, effective FROM employee WHERE now() <@ asserted ORDER BY "emp_name", "effective";

-- CREATE OR REPLACE FUNCTION perl_row() RETURNS RECORD AS $$
--     return {f2 => 'hello', f1 => 1, f3 => '["Wed Oct 06 00:00:00 2010 PDT","Fri Oct 08 00:00:00 2010 PDT")'};
-- $$ LANGUAGE plperl;

-- SELECT * FROM perl_row()
-- AS (f2 text,
--         f1 integer,
--         f3 tstzrange);