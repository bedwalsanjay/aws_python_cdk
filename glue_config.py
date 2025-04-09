#!/usr/bin/env python3
# flake8: noqa: E501,E203
glue_job_metadata = [
        # "job_type"           : "scheduled",
        # "cron_schedule"      : "cron(00 01 * * ? *)",

    # 1
    {
        "file_type"          : "file_type_1",
        "worker_type"        : "G.2X",
        "number_of_workers"  : 3,
        "redshift_table_name": "db.file_type_1",
        "job_type"           : "event_based",
    },
    # 2
    {
        "file_type"          : "file_type_2",
        "worker_type"        : "G.1X",
        "number_of_workers"  : 2,
        "redshift_table_name": "db.file_type_2",
        "job_type"           : "event_based",
    }
]