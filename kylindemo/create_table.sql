DROP TABLE IF EXISTS employee;

CREATE TABLE employee(
id int,
name string,
deptId int,
age int,
salary float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


DROP TABLE IF EXISTS department;

CREATE TABLE department(
id int,
name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


LOAD DATA INPATH '/tmp/data/kylin/employee.csv' OVERWRITE INTO TABLE employee;
LOAD DATA INPATH '/tmp/data/kylin/department.csv' OVERWRITE INTO TABLE department;
