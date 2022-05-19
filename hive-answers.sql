Create table titles( title_id Varchar(20) , title varchar(30))
STORED AS PARQUET LOCATION "/user/anabig114251/EXL/titles";
--
create table employees( emp_no int ,  emp_title_id varchar(10) ,  birth_date varchar(15),  first_name varchar(20) ,  last_name varchar(20) ,  sex varchar(2) ,  hire_date varchar(15) ,  no_of_projects int ,  last_performance_rating varchar(4) ,  left_ tinyint ,  last_date Varchar(15))
STORED AS PARQUET LOCATION "/user/anabig114251/EXL/employees";
--
create table salaries( emp_no int , salary int)
STORED AS PARQUET LOCATION "/user/anabig114251/EXL/salaries";
--
create table department_employees ( emp_no int, dept_no varchar(10))
STORED AS PARQUET LOCATION "/user/anabig114251/EXL/department_employees";
--
create table department_manager ( dept_no Varchar(10), emp_no int)
STORED AS PARQUET LOCATION "/user/anabig114251/EXL/department_manager";
--
create table departments ( dept_no varchar(10), dept_name varchar(30))
STORED AS PARQUET LOCATION "/user/anabig114251/EXL/departments";

--used web shell for query use limit for saving time

--1- List the following details of each employee: employee number, last name, first name, sex, and salary. 
select s.emp_no, e.last_name, e.first_name, e.sex, s.salary
from employees as e
inner join salaries as s
on s.emp_no = e.emp_no
order by s.emp_no;

--2-List first name, last name, and hire date for employees who were hired in 1986.

select emp_no, last_name, first_name, hire_date 
from employees 
where year(to_date(from_unixtime(unix_timestamp(hire_date, 'dd/MM/yyyy')))) = 1986;


--3-List the manager of each department with the following information: department number, department name, the manager's employee number, last name, first name.

SELECT departments.dept_no, departments.dept_name, department_manager.emp_no, employees.last_name, employees.first_name 
FROM departments 
JOIN department_manager  ON departments.dept_no = department_manager.dept_no  
JOIN employees  ON department_manager.emp_no = employees.emp_no ;

--4-List the department of each employee with the following information: employee number, last name, first name, and department name.

SELECT departments.dept_no, departments.dept_name, department_manager.emp_no, employees.last_name, employees.first_name
FROM departments
JOIN department_manager
ON departments.dept_no = department_manager.dept_no
JOIN employees
ON department_manager.emp_no = employees.emp_no;
                                                                                                                                                                      

--5-List first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B."

SELECT employees.first_name, employees.last_name, employees.sex
FROM employees
WHERE first_name = 'Hercules'
AND last_name Like 'B%';

--6-A list showing all employees in the Sales department, including their employee number, last name, first name, and department name.

select t1.emp_no,t1.last_name,t1.first_name,t3.dept_name from employees t1 inner join department_employees t2 on t1.emp_no = t2.emp_no 
  inner join departments t3 on t2.dept_no = t3.dept_no 
  where t3.dept_name = '"Sales"'

--7-List all employees in the Sales and Development departments, including their employee number, last name, first name, and department name.

select t1.emp_no,t1.last_name,t1.first_name,t3.dept_name from employees t1 inner join department_employees t2 on t1.emp_no = t2.emp_no 
  inner join departments t3 on t2.dept_no = t3.dept_no 
  where t3.dept_name = '"Sales"'
  OR departments.dept_name = 'Development';

--8-A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each last name 

SELECT last_name,
COUNT(last_name) AS "frequency"
FROM employees
GROUP BY last_name
ORDER BY
COUNT(last_name) DESC;  

