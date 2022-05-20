# Data_eng

Data Engineering
Introduction:
• You have been hired as a new data engineer at Analytixlabs. Your first major task is to work on data engineering
project for one of the big corporation’s employees data from the 1980s and 1995s. All the database of employees
from that period are provided six CSV files. In this project, you will design data model with all the tables to hold data,
import the CSVs into a SQL database, transfer SQL database to HDFS/Hive, and perform analysis using
Hive/Impala/Spark/SparkML using the data and create data and ML pipelines.
Project Description:
In this project, you are required to create end to end data pipeline and analyzing the data.
Technology Stack:
you are required to work on below technology Stack.
- MySQL (to create database)
- Linux Commands
- Sqoop (Transfer data from MySQL Server to HDFS/Hive)
- HDFS (to store the data)
- Hive (to create database)
- Impala (to perform the EDA)
- SparkSQL (to perform the EDA)
- SparkML (to perform model building)


Project Objective: As part of this project, you are required to work on
1. Create data model as per your understanding from the data (you are required include tables names, relation between
tables, column names, data types, primary & foreign keys etc.)
Tip: You can create ER diagram either in EXCEL or using the link https://www.quickdatabasediagrams.com/ (Preferably in this app)
2. Create database & tables in MySQL server as per the above ER Diagram
3. Create Sqoop job to transfer the data from MySQL to HDFS (Data required to store in Parque/Avro/Json format)
4. Create database in Hive as per the above ER Diagram and load the data into Hive tables
5. Work on Exploratory data analysis as per the analysis requirement using Impala and Spark SQL (expecting to get the data
from hive tables).
6. Build ML Model as per the requirement.
7. Create entire data pipeline and ML pipe line
8. Upload the entire project repository including source code to Github (you are required to create github account if you
don’t have it)

SQL commmands 

***** Creating tables

Create table titles( title_id Varchar(20) unique not null, title varchar(30) not null );
create table employees( emp_no int primary key,  emp_title_id varchar(10) not null,  birth_date DATE not null,  first_name varchar(20) not null,  last_name varchar(20) not null,  sex varchar(2) not null,  hire_date DATE not null,  no_of_projects int not null,  last_performance_rating varchar(4)not null,  left_ tinyint(1) not null,  last_date DATE);
create table salaries( emp_no int unique not null, salary int not null ); 
create table department_employees ( emp_no int not null, dept_no varchar(10) not null );
create table department_manager ( dept_no Varchar(10) not null, emp_no int not null );
create table departments ( dept_no varchar(10) unique not null, dept_name varchar(30) not null );

****** loading table

load data local infile '/home/anabig114251/Data/titles.csv' into table titles fields terminated by ',' ignore 1 rows;                                     
load data local infile '/home/anabig114251/Data/employees.csv' into table employees fields terminated by ',' ignore 1 rows;                                     
load data local infile '/home/anabig114251/Data/salaries.csv' into table salaries fields terminated by ',' ignore 1 rows;                                     
load data local infile '/home/anabig114251/Data/dept_emp.csv' into table department_employees fields terminated by ',' ignore 1 rows;                                     
load data local infile '/home/anabig114251/Data/dept_manager.csv' into table department_manager fields terminated by ',' ignore 1 rows;                                     
load data local infile '/home/anabig114251/Data/departments.csv' into table departments fields terminated by ',' ignore 1 rows; 




******** Sqoop ******* Storing data in HDFS as avro using sqoop

sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114251 --username anabig114251 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir /user/anabig114251/EXL --m 1 --driver com.mysql.jdbc.Driver 

                                    
****** Commands ***** use limit at end of each command for saving time *****


******** Hive


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


--9. Histogram to show the salary distribution among the employees

select t1.emp_no, t2.emp_no, t1.first_name, t1.last_name, t1.sex, t2.salary
from employees t1
inner join salaries t2 on t1.emp_no = t2.emp_no

![employee salary](https://user-images.githubusercontent.com/105859216/169431103-485f2dea-7bda-470e-b793-0b4d715d7014.png)


--10 Bar graph to show the Average salary per title (designation)

select t1.title, avg(t3.salary) as avg_salary
from tittles t1 
inner join employees t2 on t1.title_id = t2.emp_title_id
inner join salaries t3 on t2.emp_no=t3.emp_no  
group by t1.title ;

![salary per gender](https://user-images.githubusercontent.com/105859216/169431121-a7cb107a-6fd7-4cef-a9c3-74c28b7f35ec.png)


--11 Calculate employee tenure & show the tenure distribution among the employees

select emp_no, cast(to_date(from_unixtime(unix_timestamp(hire_date, 'dd/MM/yyyy')))) as started,
       cast(to_date(from_unixtime(unix_timestamp(last_date, 'dd/MM/yyyy')))) as ended,
       year(cast(to_date(from_unixtime(unix_timestamp(last_date, 'dd/MM/yyyy')))))-year(cast(to_date(from_unixtime(unix_timestamp(hire_date, 'dd/MM/yyyy')))))
from employees;
