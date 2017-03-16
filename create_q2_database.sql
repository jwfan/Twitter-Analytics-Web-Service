-- mysql -u <andrewId> --password=CClxfree --local-infile=1 < create_q2_database.sql

-- Step 1 create database
drop database if exists q2_db;
create database q2_db;
use q2_db;

-- Step 2 create twitter table
drop table if exists `twitter`;
create table `twitter` (
	`unique_id` varchar(10) not null,
	`hashtag` varchar(140) not null,
	`user_id` varchar(19) not null,
	`keywords` LONGTEXT not null
	primary key (unique_id)
);

-- Step 3 add data to businesses table
load data local infile 'part-00000' into table q2_table columns terminated by '\t' LINES TERMINATED BY '\n';

-- Step 4 create index
create index hashtag_user_index on twitter (hashtag, user_id);
