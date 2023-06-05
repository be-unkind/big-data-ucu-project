CREATE KEYSPACE project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE project;

CREATE TABLE domains (
    domain text, 
    PRIMARY KEY domain
);
