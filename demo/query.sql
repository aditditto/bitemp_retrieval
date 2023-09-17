SELECT name, budget, mgrid, effective FROM public.dept WHERE now() <@ asserted;
SELECT id, name, salary, gender, dob, deptname, effective FROM public.emp WHERE now() <@ asserted;
SELECT name, empid, effective FROM public.skills WHERE now() <@ asserted;

-- Departemen mana yang memiliki manager dengan masa bekerja yang paling pendek?
SELECT name FROM unitemp_coalesce_table_effective(
  'public', 'dept', 
  ARRAY['name', 'mgrid']
  )
  AS (
    name VARCHAR(30),
    mgrid CHAR(2),
    effective tstzrange
  ) ORDER BY interval_len(effective) ASC LIMIT 1;

SELECT id, deptname, effective FROM emp WHERE now() <@ asserted;

-- Siapa yang pernah bekerja di departemen Book dalam satu waktu 
-- setidaknya selama durasi yang sama dengan Di atau lebih?
SELECT * FROM unitemp_coalesce_table_effective('public', 'emp', 
  ARRAY['id', 'deptname'])
AS ce1 (
  id CHAR(2),
  deptname VARCHAR(30),
  effective tstzrange
) JOIN unitemp_coalesce_table_effective('public', 'emp', 
  ARRAY['id', 'deptname'])
AS ce2 (
  id CHAR(2),
  deptname VARCHAR(30),
  effective tstzrange
) ON ce1.deptname = ce2.deptname
WHERE ce2.id = 'DI' AND ce1.deptname='Book' AND 
interval_len(ce1.effective) >= interval_len(ce2.effective);

-- Karyawan mana yang memiliki durasi paling lama dimana gajinya tidak berubah?
SELECT * FROM unitemp_coalesce_table_effective(
  'public','emp', 
  ARRAY['id', 'salary'])
AS ce1 (
  id CHAR(2),
  salary numeric,
  effective tstzrange
) ORDER BY interval_len(effective) DESC LIMIT 1;

-- Cari gaji Ed ketika dia bekerja di departemen yang sama dengan Di
SELECT DISTINCT e1.salary FROM 
emp e1 JOIN emp e2 ON 
  interval_joinable(e1.effective, e2.effective) AND
  e1.deptname = e2.deptname
WHERE intervals_contains_now(ARRAY[e1.asserted, e2.asserted]) AND
  e1.id = 'ED' AND e2.id = 'DI';

-- Kapan departemen Toy memiliki budget diatas 175 selama lebih dari satu tahun, 
-- berapa budgetnya, dan siapa manager pada saat itu?
SELECT emp.name, cdept.budget, cdept.effective FROM 
  unitemp_coalesce_table_effective('public', 'dept',
    ARRAY['name', 'budget']) AS cdept (
  name TEXT, budget numeric, effective tstzrange)
JOIN dept ON dept.name = cdept.name AND 
  interval_joinable(dept.effective, cdept.effective)
JOIN emp ON dept.mgrid = emp.id AND 
  interval_joinable(emp.effective, cdept.effective)
WHERE TRIM(cdept.name) = 'Toy' AND cdept.budget > 175 AND
  interval_len(cdept.effective) >= '1 year'::interval AND
  intervals_contains_now(ARRAY[dept.asserted, emp.asserted]);

-- Skill baru apa yang ED dapatkan setelah ia mengubah namanya menjadi ‘Edward’?
WITH change_ts AS (
  SELECT LOWER(emp.effective) AS ts FROM emp WHERE emp.id = 'ED' AND
  TRIM(name) = 'Edward' AND interval_contains_now(emp.asserted)
  ORDER BY LOWER(emp.effective) LIMIT 1
)
SELECT skills.name FROM skills WHERE empid = 'ED' AND
interval_contains_now(skills.asserted) AND 
LOWER(skills.effective) >= (SELECT ts FROM change_ts) AND
skills.name NOT IN (
  SELECT s2.name FROM skills s2 WHERE 
  interval_contains_now(s2.asserted) AND
  LOWER(s2.effective) < (SELECT ts FROM change_ts)
);

-- Cari durasi terlama dimana total budget untuk semua departemen di atas 100
SELECT effective, interval_len(effective) FROM 
unitemp_coalesce_select_effective($$
  SELECT effective FROM ita_now('public','dept',
  '{}',
  ARRAY['SUM'],
  ARRAY['budget'],
  ARRAY['sum_budget']
  ) AS (sum_budget numeric, effective tstzrange) 
  WHERE sum_budget > 100
$$, '{}') AS (effective tstzrange) 
ORDER BY interval_len(effective) DESC LIMIT 1;