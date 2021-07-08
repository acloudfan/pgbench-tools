DROP TABLE IF EXISTS test_pmon_metrics_data;
CREATE TABLE test_pmon_metrics_data (
    server text NOT NULL,
    set int NOT NULL,
    test integer NOT NULL,
    events json NOT NULL
);

DROP TABLE IF EXISTS pmon_metric_stripped;
CREATE TABLE pmon_metric_stripped (
    collected timestamp,
    timestamp bigint,
    server text NOT NULL,
    set int NOT NULL,
    test integer NOT NULL,
    clients  integer NOT NULL,
    scale    integer NOT NULL,
    category  varchar(50) NOT NULL,
    metric text NOT NULL,
    value   float,
    values  text
);

DROP TABLE IF EXISTS pmon_metric_processed;
CREATE TABLE pmon_metric_processed (
    server text NOT NULL,
    set int NOT NULL,
    clients  integer NOT NULL,
    scale    integer NOT NULL,
    category  varchar(50) NOT NULL,
    metric text NOT NULL,
    value_avg   float,
    value_min   float,
    value_max   float
);