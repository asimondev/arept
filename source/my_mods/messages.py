from __future__ import print_function
from __future__ import absolute_import


def print_templates_help():
    text = """Available AREPT templates:
SQL Trace:
 - my_sql_trace : Activate SQL tracing for the current session. 
 - ses_sql_trace [SID Serial#]
    * Activate SQL tracing for other session.
 
SQL Statement details:
 - sql_details: Get DBMS_SQLTUNE.REPORT_SQL_DETAIL() output for SQL statement.
 - get_sql_id : Get SQL_ID, child number etc for the specified SQL statement.
 
SQL Monitor:
 - awr_sql_monitor     : Get SQL Monitor output from AWR.
 - awr_sql_monitor_list: Get list of available SQL statements in AWR with SQL
                         Monitor details.
 - sql_monitor         : Get SQL Monitor.
 - sql_monitor_list    : Get list of available SQL statements with SQL Monitor
                         details.
 
SQL Profile:
 - sql_profile    : Create SQL profile.
 - awr_sql_profile: Create SQL profile based on SQL statement in AWR.
 
SQL Plan Baseline:
 - sql_baseline   : Create SQL Plan baseline.
 - awr_baseline   : Create SQL Plan baseline based on SQL statement in AWR.
 - hinted_baseline: Create SQL Plan baseline. Use hints or optimizer parameters
                    to improve the execution plan. Add this plan to the created 
                    baseline.
                    
Others:
 - process [SID Serial# Instance_Number=1]
    * Select background process ID (SPID) and other details for a 
    database session.
 - meta_table       : Get table/index/constraint DDLs using DBMS_METADATA for 
                      a table.
 - get_awr_snap_ids : Get AWR snapshot interval IDs for specified time interval. 
 - hidden_parameters: Select database hidden parameters. 
                    
"""
    print(text)

