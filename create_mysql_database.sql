-- rename the output of the map-reduce of q2 and q3 respectively first 
-- mysql -u username --password=CClxfree --local-infile=1 < create_q2_database.sql

-- Step 1 create database
drop database if exists twitter_db;
create database twitter_db;
use twitter_db;

-- Step 2 create q2 table
drop table if exists `q2_table`;
create table `q2_table` (
	`unique_id` varchar(10) not null,
	`hashtag` varchar(140) not null,
	`user_id` varchar(15) not null,
	`keywords` LONGTEXT not null
	primary key (unique_id)
);

-- Step 3 add data to q2 table
load data local infile 'q2-output' into table q2_table columns terminated by '\t' LINES TERMINATED BY '\n';

-- Step 4 create index
create index hashtag_user_index on q2_table (hashtag, user_id);

-- Step 5 create q3 table
drop table if exists `q3_table`;
create table `q3_table` (
	`twitter_id` varchar(20) not null,
	`user_id` varchar(15) not null,
	`time_id` varchar(12) not null,
	`censored_text` LONGTEXT not null,
	`impact_score` integer default 0 not null,
	`keywords` LONGTEXT not null,
	`word_count` integer default 0 not null
	primary key (twitter_id)
);

-- Step 6 add data to q3 table
load data local infile 'q3-output' into table q3_table columns terminated by '\t' LINES TERMINATED BY '\n';

-- Step 7 create index
create index user_index on q3_table (user_id);
create index time_index on q3_table (time_id);