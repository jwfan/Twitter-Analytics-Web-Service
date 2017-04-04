-- mysql -uroot --password=CClxfreee --local-infile=1 < create_q3_database.sql

-- Step 1 create database
drop database if exists q2_db;
create database q2_db default charset utf8 COLLATE utf8_general_ci;
use q2_db;

-- Step 2 create twitter table
drop table if exists q2_table;

create table q2_table (
    u_id int not null auto_increment,
	hashtag varbinary(140) not null,
	user_id varchar(19) not null,
	keywords LONGTEXT not null,
	primary key (u_id)
);

-- Step 3 add data to twitter table
load data local infile 'part-00000' into table q2_table columns terminated by '\t' LINES TERMINATED BY '\n' (hashtag, user_id, keywords);

-- Step 4 create index
create index hashtag_user_index on q2_table (hashtag, user_id);