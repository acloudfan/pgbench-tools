DROP TABLE IF EXISTS test_pmon_metrics_data;
CREATE TABLE test_pmon_metrics_data (
    set int NOT NULL,
    test integer NOT NULL,
    metric json NOT NULL,
    server text NOT NULL
)
