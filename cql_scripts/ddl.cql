CREATE KEYSPACE wiki_data WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wiki_data;

CREATE TYPE domain_hour_stats (
  domain TEXT,
  created_pages INT
);

CREATE TYPE user_hour_stats (
    user_name TEXT,
    user_id INT,
    created_pages INT,
    page_titles list<text>
);

CREATE TABLE domain_table (
    domain TEXT,
    page_id INT,
    PRIMARY KEY ((domain), page_id)
);

CREATE TABLE user_table (
    user_id INT,
    page_id INT,
    page_title TEXT,
    PRIMARY KEY (user_id)
);

CREATE TABLE user_time_table (
    user_id INT,
    user_text TEXT,
    page_id INT,
    rev_timestamp TIMESTAMP,
    page_title TEXT,
    PRIMARY KEY ((rev_timestamp), user_id, user_text)
);

CREATE TABLE page_table (
    page_id INT,
    page_title TEXT,
    PRIMARY KEY (page_id)
);

CREATE TABLE page_time_table (
    rev_timestamp TIMESTAMP,
    domain TEXT,
    page_id INT,
    user_is_bot BOOLEAN,
    PRIMARY KEY ((rev_timestamp), page_id)
);

CREATE TABLE domain_stats (
    start_hour TIMESTAMP,
    end_hour TIMESTAMP,
    statistics list<frozen<domain_hour_stats>>,
    PRIMARY KEY (start_hour)
);

CREATE TABLE domain_bot_stats (
    start_hour TIMESTAMP,
    end_hour TIMESTAMP,
    statistics list<frozen<domain_hour_stats>>,
    PRIMARY KEY (start_hour)
);

CREATE TABLE user_stats (
    start_hour TIMESTAMP,
    end_hour TIMESTAMP,
    statistics list<frozen<user_hour_stats>>,
    PRIMARY KEY (start_hour)
);