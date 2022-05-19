SQL commmands 

--Creating tables

Create table titles( title_id Varchar(20) unique not null, title varchar(30) not null );
create table employees( emp_no int primary key,  emp_title_id varchar(10) not null,  birth_date DATE not null,  first_name varchar(20) not null,  last_name varchar(20) not null,  sex varchar(2) not null,  hire_date DATE not null,  no_of_projects int not null,  last_performance_rating varchar(4)not null,  left_ tinyint(1) not null,  last_date DATE);
create table salaries( emp_no int unique not null, salary int not null ); 
create table department_employees ( emp_no int not null, dept_no varchar(10) not null );
create table department_manager ( dept_no Varchar(10) not null, emp_no int not null );
create table departments ( dept_no varchar(10) unique not null, dept_name varchar(30) not null );

