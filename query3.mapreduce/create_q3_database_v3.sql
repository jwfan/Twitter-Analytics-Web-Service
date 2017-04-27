-- mysql -uroot --password=CClxfreee --local-infile=1 < create_q3_database.sql

-- Step 1 create database
drop database if exists q3_db;
create database q3_db;
ALTER DATABASE q3_db CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
use q3_db;

-- Step 2 create twitter table
drop table if exists q3_table;

create table q3_table1 (
	twitter_id varchar(20) not null,
	time_stamp BIGINT not null,
	user_id BIGINT not null,
	censored_text LONGTEXT not null,
	impact_score integer default 0 not null,
	keywords LONGTEXT not null,
	primary key (user_id,time_stamp)
	)partition by hash(time_stamp)
partitions 6;

ALTER TABLE q3_table1 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE q3_table1 CHANGE censored_text censored_text LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE q3_table1 CHANGE keywords keywords LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Step 3 add data to twitter table
load data local infile 'part0' into table q3_table1 columns terminated by '\t' LINES TERMINATED BY '\n';

-- Step 4 create index
//create index user_index on q3_table (user_id);
create index time_index on q3_table (time_stamp);
//create index tuid on q3_table (time_stamp,user_id);
