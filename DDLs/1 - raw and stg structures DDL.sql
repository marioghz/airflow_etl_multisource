CREATE DATABASE test;


CREATE TABLE IF NOT EXISTS raw_olympics (
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
medal varchar
);

CREATE TABLE IF NOT EXISTS stg_olympics (
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
medal varchar
);


CREATE TABLE IF NOT EXISTS raw_student (	
ID		int
,student_name	varchar
);

CREATE TABLE IF NOT EXISTS stg_covid_data (
"date" date
,case_count int
,hospitalized_count int
,death_count int
);
