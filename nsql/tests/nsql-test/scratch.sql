CREATE TABLE employee (
    id int PRIMARY KEY,
    name text,
    department text,
    salary int
);

INSERT INTO employee (id, name, department, salary)
VALUES (1, 'Alice', 'HR',          50),
       (2, 'Bob',   'Engineering', 100),
       (3, 'Carol', 'Engineering', 70),
       (4, 'Dan',   'Engineering', 60),
       (5, 'Eve',   'HR',          50),
       (6, 'Frank', 'Engineering', 80);

SELECT id,
       name,
       (SELECT AVG(salary)
          FROM employee
       WHERE department = emp.department) AS department_average
   FROM employee emp;
