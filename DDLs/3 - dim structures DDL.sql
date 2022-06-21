CREATE TABLE IF NOT EXISTS dim_student (	
ID		int
,student_name	varchar
,start_date date
,end_date date,
is_current boolean
);


CREATE TABLE IF NOT EXISTS dim_covid_data (
"date" date
,case_count int
,hospitalized_count int
,death_count int
,start_date date 
,end_date date 
,is_current boolean 
);


CREATE TABLE IF NOT EXISTS dim_olympics (
id int,
name varchar,
sex bpchar(1),
age int,
height int,
weight int,
team varchar,
noc varchar,
games varchar,
year int,
season varchar,
city varchar,
sport varchar,
event varchar,
medal varchar,
start_date date,
end_date date,
is_current bool
);