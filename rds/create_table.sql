DROP TABLE IF EXISTS test_pmon_metrics_data;
CREATE TABLE test_pmon_metrics_data (
    collected TIMESTAMP,
    value float,
    metric text NOT NULL,
    test integer NOT NULL,
    server text NOT NULL
)
