-- mysql -u <andrewId> --password=CClxfree --local-infile=1 < create_q3_database.sql

-- Step 1 create database
drop database if exists q3_db;
create database q3_db;
use q3_db;

-- Step 2 create twitter table
drop table if exists `twitter`;
create table `twitter` (
	`twitter_id` varchar(20) not null,
	`user_id` varchar(19) not null,
	`time_stamp` varchar(13) not null,
	`censored_text` LONGTEXT not null,
	`impact_score` integer default 0 not null,
	`keywords` LONGTEXT not null,
	`word_count` integer default 0 not null
	primary key (twitter_id)
);

-- Step 3 add data to twitter table
load data local infile 'part-00000' into table twitter columns terminated by '\t' LINES TERMINATED BY '\n';

-- Step 4 create index
create index user_index on twitter (user_id);
create index time_index on twitter (time_stamp);
