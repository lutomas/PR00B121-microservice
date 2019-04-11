--

-- CREATE KEYSPACE IF NOT EXISTS PR00B121_microservice
--     WITH REPLICATION = {
--     'class' : 'SimpleStrategy',
--     'replication_factor' : 1
--     }


DROP TABLE IF EXISTS account;
CREATE TABLE account
(
    id   timeuuid, // Timestamp from when this configuration is active.
    name text,     // Configuration name.
    PRIMARY KEY (id)
);
