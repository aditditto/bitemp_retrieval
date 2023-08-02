DROP EXTENSION bitemp_retrieval;
CREATE EXTENSION bitemp_retrieval;
SET DATESTYLE TO 'ISO';
SET TIMEZONE TO 'Asia/Jakarta';

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
    'public',
    'emp',
    $$id serial, 
      name varchar(30) not null,
      salary numeric,
      gender character(1),
      bod date,
      deptname char(30)
    $$,
   'emp_id');

DROP EX