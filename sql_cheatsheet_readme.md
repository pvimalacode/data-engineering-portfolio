````yaml
# SQL Tutorial Notes

## Table Creation

- Create table with primary key
  ```sql
  CREATE TABLE IF NOT EXISTS projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(100)
  );
````

> Note: MySQL does not have `REPLACE`, use `DROP IF EXISTS` before `CREATE` Primary Key cannot be NULL

- Create table with composite key and foreign key:
  ```sql
  CREATE TABLE emp (
    employee_id INT,
    department_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    hire_date DATE,
    project_assigned BOOLEAN,
    project_id INT,
    PRIMARY KEY (employee_id, department_id),
    CONSTRAINT fk_project FOREIGN KEY (project_id)
      REFERENCES projects(project_id)
      ON DELETE SET NULL
      ON UPDATE CASCADE
  );
  ```

## Foreign Key Options

- `CASCADE`: Updates/deletes child rows
- `SET NULL`: Sets to NULL on parent delete/update
- `SET DEFAULT`: Sets to column default on delete/update
- `RESTRICT`: Prevents parent delete/update if child exists
- `NO ACTION`: Checks constraints at end of transaction

## Default Value for a Field

```sql
CREATE TABLE IF NOT EXISTS projects (
  project_id INT PRIMARY KEY,
  project_name VARCHAR(100) DEFAULT 'project name'
);
```

## Views

```sql
CREATE OR REPLACE VIEW emp_project_view AS
SELECT
  e.employee_id,
  e.project_id,
  p.project_name,
  e.manager_id AS mgrid
FROM emp e
LEFT JOIN projects p ON e.project_id = p.project_id;
```

## Partitioning (MySQL)

### Notes:

- Must use primary or part of composite key
- Not useful for joins
- Good for huge tables (>1B rows)

### Types:

- `RANGE`: Works with expressions like `YEAR(date)`
- `LIST`: Integer only (e.g., region\_id)
- `HASH`: Based on `hash(col) % n`
- `KEY`: Like HASH but auto-generated algorithm

### Range Partition

```sql
PARTITION BY RANGE (YEAR(hire_date)) (
  PARTITION p_before_2015 VALUES LESS THAN (2015),
  PARTITION p_2015_2019 VALUES LESS THAN (2020),
  PARTITION p_2020_2024 VALUES LESS THAN (2025),
  PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

### List Partition

```sql
PARTITION BY LIST (department_id) (
  PARTITION p_dept_a VALUES IN (10, 20),
  PARTITION p_dept_b VALUES IN (30),
  PARTITION p_other VALUES IN (40, 50, 60)
);
```

### Hash Partition

```sql
PARTITION BY HASH (employee_id)
PARTITIONS 4;
```

### Key Partition

```sql
PARTITION BY KEY (email_id)
PARTITIONS 4;
```

## Indexing

- Improves search speed
- Types:
  - Clustered (only one): data stored in index order
  - Non-clustered: separate structure
  - Composite Index: multi-column
  - Expression-based: use generated column

```sql
-- Unique index
UNIQUE INDEX idx_email (email_id);

-- Non-clustered index
INDEX idx_email (email_id);

-- Composite index
CREATE INDEX idx_name_email ON employee (last_name, first_name, email_id);

-- Expression-based index (MySQL):
ALTER TABLE employee ADD COLUMN email_domain VARCHAR(100) AS (SUBSTRING_INDEX(email_id, '@', -1)) STORED;
CREATE INDEX idx_email_domain ON employee (email_domain);
```

## Insert & Update

```sql
-- Insert
INSERT INTO projects (project_id, project_name)
VALUES (1, 'Project Apollo'), (2, 'Project Zeus'), (3, 'Project Athena');

-- Update
UPDATE projects SET project_name = 'Project Hermes' WHERE project_id = 2;

-- Update with CASE
UPDATE employee
SET mgr_id = CASE emp_id
  WHEN 2 THEN 1
  WHEN 3 THEN 1
  WHEN 4 THEN 2
  WHEN 5 THEN 2
  WHEN 6 THEN 3
END
WHERE emp_id IN (2, 3, 4, 5, 6);
```

## Select & Where

```sql
SELECT project_id, project_name FROM projects WHERE project_id = 2;
```

## Joins

| Join Type  | Description                   |
| ---------- | ----------------------------- |
| INNER JOIN | Matches in both               |
| LEFT JOIN  | All from left + match         |
| RIGHT JOIN | All from right + match        |
| FULL OUTER | UNION of LEFT + RIGHT         |
| CROSS JOIN | Cartesian (every row x every) |

Note: MySQL doesn’t support FULL JOIN natively — use UNION.

## Running Total

```sql
SELECT *, SUM(sales) OVER (ORDER BY id) AS running_total FROM t1;
```

## Hierarchical Managers (Recursive CTE)

```sql
WITH RECURSIVE mgr AS (
  SELECT *, 0 AS level FROM employee WHERE emp_id = 1
  UNION ALL
  SELECT e.*, m.level + 1
  FROM employee e
  INNER JOIN mgr m ON m.emp_id = e.mgr_id
)
SELECT * FROM mgr;
```

## Find Missing Numbers

```sql
WITH RECURSIVE nos AS (
  SELECT 1 AS no
  UNION ALL
  SELECT no + 1 FROM nos WHERE no < (SELECT MAX(col1) FROM tab1)
)
SELECT nos.no FROM nos
LEFT JOIN tab1 ON nos.no = tab1.col1
WHERE tab1.col1 IS NULL;
```

## Available Sequential Seats (Status)

```sql
WITH cte AS (
  SELECT *, col1 - ROW_NUMBER() OVER (ORDER BY col1) AS sts
  FROM tab1 WHERE status = 0
),
cte2 AS (
  SELECT col1, COUNT(*) OVER (PARTITION BY sts) AS cnt
  FROM cte
)
SELECT col1 AS seats FROM cte2 WHERE cnt > 1;
```

## EXISTS vs JOIN

- `JOIN`: readable but can be slower
- `EXISTS`: faster for filtering

```sql
SELECT s.*, t.id, t.name, a.id, g.grade
FROM students s, teacher t, assignment a, grade g
WHERE EXISTS (SELECT 1 FROM teacher WHERE t.s_id = s.id)
AND EXISTS (SELECT 1 FROM assignment WHERE a.t_id = t.id)
AND EXISTS (SELECT 1 FROM grade WHERE g.s_id = s.id AND g.t_id = t.id AND g.a_id = a.id);
```

## ANY / ALL

```sql
-- ANY: at least one
SELECT * FROM products WHERE price = ANY (SELECT price FROM sales WHERE discount > 0);

-- ALL: must meet all
SELECT * FROM employees
WHERE salary > ALL (SELECT salary FROM employees WHERE department = 'HR');
```

## DELETE vs TRUNCATE vs DROP

```sql
DELETE FROM table_name WHERE condition; -- deletes rows
TRUNCATE TABLE table_name;              -- deletes all rows, keeps schema
DROP TABLE table_name;                  -- removes table completely
```

## Row Limits

```sql
-- Top
SELECT * FROM emp LIMIT 10;
-- Bottom
SELECT * FROM emp ORDER BY salary DESC LIMIT 10;
-- Middle
SELECT * FROM emp LIMIT 10 OFFSET 3;
```

## Duplicates

```sql
SELECT id, COUNT(*) AS cnt FROM emp GROUP BY id HAVING cnt > 1;
```

## Second Highest Salary

```sql
SELECT * FROM emp ORDER BY salary DESC LIMIT 1 OFFSET 1;
```

## Rankings

```sql
SELECT grades, s_id,
  DENSE_RANK() OVER (ORDER BY grades) AS school_rank,
  RANK() OVER (ORDER BY grades) AS public_school,
  ROW_NUMBER() OVER (ORDER BY grades) AS govt
FROM grade;
```

## UNION vs UNION ALL

- `UNION`: no duplicates
- `UNION ALL`: includes duplicates

## ORDER BY vs SORT BY (Spark)

- `ORDER BY`: global sort
- `SORT BY`: sort within partitions only

## PARTITION BY vs COALESCE

- `PARTITION BY`: causes shuffle
- `COALESCE`: avoids shuffle

## Pivot

- Not supported in MySQL — use CASE + GROUP BY
- Native in SQL Server via `PIVOT`

```sql
SELECT region, [2023], [2024]
FROM (
  SELECT region, year, amount FROM sales
) AS src
PIVOT (
  SUM(amount) FOR year IN ([2023], [2024])
) AS pvt;
```

```
```
