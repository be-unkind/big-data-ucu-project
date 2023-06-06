CREATE KEYSPACE project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE project;

CREATE TABLE domains (
    domain text, 
    PRIMARY KEY (domain)
);

CREATE TABLE page_titles (
    page_title text, 
    PRIMARY KEY (page_title)
);