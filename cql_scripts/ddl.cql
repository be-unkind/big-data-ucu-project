CREATE KEYSPACE wiki_data WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wiki_data;

CREATE TABLE domain_table (
    domain TEXT,
    page_id INT,
    PRIMARY KEY ((domain), page_id)
);

CREATE TABLE user_table (
    user_id INT,
    page_id INT,
    page_text TEXT,
    PRIMARY KEY (user_id)
);

CREATE TABLE user_time_table (
    user_id INT,
    user_text TEXT,
    page_id INT,
    rev_timestamp TIMESTAMP,
    PRIMARY KEY ((rev_timestamp), user_id)
);

CREATE TABLE page_table (
    page_id INT,
    page_text TEXT,
    PRIMARY KEY (page_id)
);
