# AREPT
AREPT Python scripts for working with Oracle databases.

This *README* describes the usage of AREPT Python scripts. AREPT provides the following
features to the users:
- Generate different reports from Oracle database. The results are written into text and HTML files. The tool uses SQL*Plus to select the data.
- Generate SQL snippets (AREPT templates) as script ouput and SQL files for SQL*Plus.

Depending on your Python version (2 or 3), you should start AREPT using `./arept.py` or 
`./arept3.py`. 

## AREPT parameters

```
./arept.py -h
 arept.py -h
Usage: arept.py [options]

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -c CONFIG, --config=CONFIG
                        JSON configuration file
  -u USER, --user=USER  database user
  -p PASSWORD, --password=PASSWORD
                        database user password
  -d DATABASE, --database=DATABASE
                        TNS alias or Easy Connect
  --pdb=PDB             PDB name
  -o OUTPUT_DIR, --output-dir=OUTPUT_DIR
                        output directory
  --output-level=OUTPUT_LEVEL
                        output level {typical | all}. Default: typical
  --output-format=OUTPUT_FORMAT
                        output format: text, html, active-html. Default:
                        text,html
  --obj=OBJ             object names: [table:][owner].name,..;index:[owner.]name,..;
                           index_only:[owner.]name,..;view:[owner.]name,...;
                           mview:[owner.]name...;source:[owner.]name,...
  --obj-file=OBJ_FILE   file name with objects
  --schema=SCHEMA       default schema for objects
  -b BEGIN_TIME, --begin-time=BEGIN_TIME
                        begin time. Format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd |
                        hh24:mi}
  -e END_TIME, --end-time=END_TIME
                        end time. Format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd |
                        hh24:mi | now}
  --awr-sql-id=AWR_SQL_ID
                        SQL_IDs in AWR
  --awr-report          Get AWR reports
  --awr-summary         Get only one AWR report for the whole interval.
  --global-awr-report   Get global AWR reports
  --global-awr-summary  Get global AWR reports
  --addm-report         Get ADDM reports
  --rt-addm-report      Get real-time ADDM report
  --ash-report          Get ASH reports
  --global-ash-report   Get global ASH reports
  --rt-perfhub-report   Get real-time performance hub report
  --awr-perfhub-report  Get AWR performance hub report
  --rt-perfhub-sql=RT_PERFHUB_SQL
                        Get real-time performance hub SQL statement report
  --awr-perfhub-sql=AWR_PERFHUB_SQL
                        Get AWR performance hub SQL statement report
  --rt-perfhub-session  Get real-time performance hub session report
  --awr-perfhub-session
                        Get AWR performance hub session report
  --instances=INSTANCES 
                        Instances list (default: all instances; 0 - current instance)                      
  --parallel=PARALLEL   Number of parallel AWR/ADDM reports
  --resource-plan       Get Resource Manager Plan
  --sql-id=SQL_ID       Cursor SQL_ID in shared library
  --sql-child-number=SQL_CHILD_NUMBER
                        Cursor child nuber in shared library
  --sql-format=SQL_FORMAT
                        Format option in DBMS_XPLAN.DISPLAY_CURSOR like basic,
                        typical, serial, all, adaptive. Default: typical
  --sid=SID             Session SID number.
  --serial=SERIAL       Session serial number.
  --instance=INSTANCE   Instance number (0 - current instance).
  --cleanup             "rm -rf *" for existing output directory
  -v, --verbose         verbose
  --begin-snap-id=BEGIN_SNAP_ID
                        min. snapshot ID
  --end-snap-id=END_SNAP_ID
                        max. snapshot ID
  --get-wait-event=GET_WAIT_EVENT
                        Get wait event parameters description
  -t TEMPLATE, --template=TEMPLATE
                        {process | my_sql_trace | ses_sql_trace | meta_table |
                        meta_role | sql_details | awr_sql_monintor |
                        awr_sql_monitor_list | sql_monitor | sql_monitor_list
                        |sql_profile | awr_sql_profile | sql_baseline |
                        awr_baseline | hinted_baseline | get_awr_snap_ids |
                        hidden_parameters | get_sql_id | sql_shared_cursor |
                        check_sql_id }
  --template-help       Show description of AREPT templates.
```

Use *--template-help* option to get short description of available SQL snippets 
(AREPT templates).

## Database Connection.

If you don't provide any connection details, AREPT would try to use SQL*Plus with 
the internal connection: `/ as sysdba`.

If you provide *--pdb PDB_Name* only, then AREPT connects as internal and changes 
session to this PDB:
```
connect / as sysdba
alter session set container=PDB_Name;
```

## AREPT Output Directory.

Per default the created files will be placed into the `./arept_output` directory. You can use *--output-directory* to change this.

## AREPT Templates.

AREPT templates are generated in 2 ways:
- SQL snippets displayed on the screen.
- SQL file for SQL*Plus.

Sometimes you have to modify the SQL statement or SQL files before running them. 
For instance, you would have to insert your SID and SERIAL# etc. 

## AREPT by Examples

### SQL Plan Details.

You can use AREPT to get SQL exection plan and other details for the SQL statement
 in the library cache:
```
./arept.py --sql-id ayfpfz93vnvt1
Output directory: /home/oracle/arept/arept_output
 - File sql_id_ayfpfz93vnvt1_xplan.txt created.
 - File sql_id_ayfpfz93vnvt1_sql.txt created.
 - File sql_id_ayfpfz93vnvt1_sql.html created.
 - File sql_id_ayfpfz93vnvt1_sqlarea.txt created.
 - File sql_id_ayfpfz93vnvt1_sqlarea.html created.
 - File sql_id_ayfpfz93vnvt1_mon_list.txt created.
 - File sql_id_ayfpfz93vnvt1_mon_report.txt created.
 - File sql_id_ayfpfz93vnvt1_detail_active.html created.
 - File sql_id_ayfpfz93vnvt1_mon_list.html created.
 - File sql_id_ayfpfz93vnvt1_mon_list_active.html created.
 - File sql_id_ayfpfz93vnvt1_mon_report.html created.
 - File sql_id_ayfpfz93vnvt1_mon_report_active.html created.
```

### AWR SQL Plan Details.

Use AREPT to get SQL exection plan and other details from AWR. The specified SQL_ID 
will be also checked in DBA_HIST_REPORTS for available SQL Monitor reports. Per default the 
reports in both HTML and text format will be generated. The option *--cleanup* will remove 
all available files in the specified output directory **test01** before starting AREPT.

```
arept.py -b "2022-11-30 21:00" -e "2022-11-30 22:15" --awr-sql-id btqubgr940awu -o test01 --cleanup
Output directory: test01
 - File test01/awr_sql_id_btqubgr940awu_inst_1_report.txt created.
 - File test01/awr_sql_id_btqubgr940awu_inst_1_report.html created.
 - File test01/awr_sql_id_btqubgr940awu_hist_reports.txt created.
 - File test01/awr_sql_id_btqubgr940awu_hist_reports.html created.

 => Use *hist_reports* files to check for available SQL Monitor reports for this SQL statement. The important columns are:
- RID(REPORT_ID): report ID of this report
- KEY1          : SQL_ID for this statement
- KEY2          : SQL execution ID for this statement
- REPORT_SUMMARY: report summary
- COMPONENT_NAME: 'sqlmonitor'

 => You can use AREPT template awr_sql_monitor to select such a report. You need REPORT_ID and KEY1 to generate such a report.
```

### Database Object Details.

Get DDLs and some other basic details about the table TEST01 from the database:
```
./arept.py --obj test01
Output directory: /home/oracle/arept/arept_output
 - File table_ANDREJ_TEST01_metadata.txt created.
 - File table_ANDREJ_TEST01.txt created.
 - File table_ANDREJ_TEST01.html created.
 - File table_ANDREJ_TEST01_indexes.txt created.
 - File table_ANDREJ_TEST01_indexes.html created.
```
For SQL tuning you need more details about the tables. For instance, you would like
to get optimizer statistics for this table etc. You should use the option *--output-level all* to get more details:

```
./arept.py --obj test01 --output-level all
Output directory: /home/oracle/arept/arept_output
 - File table_ANDREJ_TEST01_metadata.txt created.
 - File table_ANDREJ_TEST01.txt created.
 - File table_ANDREJ_TEST01.html created.
 - File table_ANDREJ_TEST01_indexes.txt created.
 - File table_ANDREJ_TEST01_indexes.html created.
 - File table_ANDREJ_TEST01_stats.txt created.
 - File table_ANDREJ_TEST01_stats.html created.
 - File table_ANDREJ_TEST01_columns_stats.txt created.
 - File table_ANDREJ_TEST01_columns_stats.html created.
 - File table_ANDREJ_TEST01_index_stats.txt created.
 - File table_ANDREJ_TEST01_index_stats.html created.
 - File table_ANDREJ_TEST01_histograms.txt created.
 - File table_ANDREJ_TEST01_histograms.html created.
 - File table_ANDREJ_TEST01_part_stats.txt created.
 - File table_ANDREJ_TEST01_part_stats.html created.
 - File table_ANDREJ_TEST01_subpart_stats.txt created.
 - File table_ANDREJ_TEST01_subpart_stats.html created.
```

Using *--obj* option you can get details not only for the tables. For instance, you can 
select the PL/SQL code as well. In the below example you would get the PL/SQL code 
for the MY_FUNC01 function from the PDB acdb19a_pdb1. 

```
./arept.py --obj="source:my_func01" --pdb acdb19a_pdb1 
Output directory: /home/oracle/arept/arept_output
 - File /home/oracle/arept/arept_output/FUNCTION_ANDREJ_MY_FUNC01_metadata.txt created.
 - File /home/oracle/arept/arept_output/FUNCTION_ANDREJ_MY_FUNC01.txt created.
 - File /home/oracle/arept/arept_output/FUNCTION_ANDREJ_MY_FUNC01.html created.
```
### SQL Tuning.

You can use AREPT templates to generate SQL snippets for SQL*Plus to work with 
SQL profiles and SQL plan baselines.

### SQL Plan Baselines.
```
./arept.py -t sql_baseline
 - File /home/oracle/arept/arept_output/create_sql_baseline.sql created.

./arept.py -t awr_baseline
 - File /home/oracle/arept/arept_output/create_awr_baseline.sql created.

```

Sometimes you want to create a baseline from existing SQL statement but to 
change the execution plan using hints or optimizer parameters. The template *hinted_baseline* provides such SQL snippets:

```
./arept.py -t hinted_baseline 
 - File /home/oracle/arept/arept_output/create_sql_baseline.sql created.
 - File /home/oracle/arept/arept_output/change_sql_baseline.sql created.
```

### SQL Profiles.

You can start SQL Tuning Advisor to get SQL profile for the SQL statement. 
The corresponding SQL statement could be either in AWR or in the current library 
cache. You have to start the advisor task and after competition check the report.
That's why different scripts will be generated:
```
./arept.py -t sql_profile     
 - File /home/oracle/arept/arept_output/start_sql_profile.sql created.
 - File /home/oracle/arept/arept_output/get_sql_profile.sql created.
```

Get SQL profile from AWR.
```
arept.py -t awr_sql_profile -o awr_prof01      
 - File awr_prof01/s01_start_task.sql created.
 - File awr_prof01/s02_get_report.sql created.
 - File awr_prof01/check_task.sql created.

 => Use the s01_start_task.sql script to start the task. The script s02_get_report.sql should be used later to fetch the ready report. You can check the running task with the script check_task.sql.

 => If you want to change the task name, don't forget to do it in all generated scripts. You can consider to adjust the default timeout value (600 seconds) for your task.

```

### Enable SQL Trace.

You can get AREPT templates for enable SQL trace both in the own session 
and in the remote session:
- Enabling SQL trace in the own session.

```
./arept.py -t my_sql_trace

set echo on pagesi 100 linesi 256 trimsp on

spool arept_my_sql_trace.log

alter session set tracefile_identifier=arept_ses_trace;

select a.sid, a.serial# ser, b.spid,
  a.con_id, a.username db_user, a.machine,
  b.program, b.tracefile, b.traceid
from v$session a, v$process b
where a.paddr = b.addr and a.con_id = b.con_id and 
  a.sid = sys_context('userenv', 'sid')
/

select a.instance_number, a.instance_name, a.host_name, a.status
from v$instance a
/

-- alter session set sql_trace=true;

exec dbms_session.session_trace_enable(waits=>true, binds=>false, plan_stat=>null);

spool off 

-- Run this command to finish the trace or disconnect the session.
-- exec dbms_session.session_trace_disable;

 - File /home/oracle/arept/arept_output/my_sql_trace.sql created.
```

- Enabling SQL trace in the remote session.
```
./arept.py -t ses_sql_trace    
        
set echo on pagesi 100 linesi 256 trimsp on verify on

spool arept_ses_sql_trace.log

select a.sid, a.serial# ser, b.spid,
  a.con_id, a.username db_user, a.machine,
  b.program, b.tracefile, b.traceid
from v$session a, v$process b
where a.paddr = b.addr and a.con_id = b.con_id and 
  a.sid = ... and a.serial# = ...
/

select a.instance_number, a.instance_name, a.host_name, a.status
from v$instance a
/
    
exec dbms_monitor.session_trace_enable(session_id=>..., serial_num=>..., waits=>true, binds=>false, plan_stat=>null);

spool off 

-- Run this command to finish the trace or disconnect the session.
-- exec dbms_monitor.session_trace_disable(session_id=>..., serial_num=>...);


 - File /home/oracle/arept/arept_output/ses_sql_trace.sql created.
```
You have to start the created SQL file *ses_sql_trace.sql* from SQL*Plus and provide
as script parameters SID and Serial# for the specific session on the same instance:

`SQL> @ses_sql_trace 497 55509`

- Enabling SQL trace in the remote session using SID and Serial#. In this case the
displayed output will contain SQL statements using the provided SID and Serial#. You 
could use these statements to start the SQL trace without making any changes.
(Copy and Paste.)

```
 ./arept.py -t ses_sql_trace 497 55509
        
set echo on pagesi 100 linesi 256 trimsp on verify on

spool arept_ses_sql_trace.log

select a.sid, a.serial# ser, b.spid,
  a.con_id, a.username db_user, a.machine,
  b.program, b.tracefile, b.traceid
from v$session a, v$process b
where a.paddr = b.addr and a.con_id = b.con_id and 
  a.sid = 497 and a.serial# = 55509
/

select a.instance_number, a.instance_name, a.host_name, a.status
from v$instance a
/
    
exec dbms_monitor.session_trace_enable(session_id=>497, serial_num=>55509, waits=>true, binds=>false, plan_stat=>null);

spool off 

-- Run this command to finish the trace or disconnect the session.
-- exec dbms_monitor.session_trace_disable(session_id=>497, serial_num=>55509);


 - File /home/oracle/arept/arept_output/ses_sql_trace.sql created.
```

### Check V$SQL_SHARED_CURSOR View.

The dynamic view *V$SQL_SHARED_CURSOR* contains reasons for not sharing a particular cursor. 
Unfortunately the output of this view is rather difficult to read. AREPT can help here as well.

At first you would generate a script using AREPT template *sql_shared_cursor*:

```
arept.py -t sql_shared_cursor
 - File /home/oracle/arept/arept_output/sql_shared_cursor.sql created.

Start /home/oracle/arept/arept_output/sql_shared_cursor.sql from SQL*Plus using SQL_ID as first parameter.
```

Then we can ran this script from SQL*Plus:

```
SQL> @arept_output/sql_shared_cursor 8swypbbr0m372

AREPT Ver. 0.3.0  (https://github.com/asimondev/arept)

Output from GV$SQL_SHARED_CURSOR:
- Inst. ID: 1 Child Number: 0  (Con-ID: 0)
- Inst. ID: 1 Child Number: 1  OPTIMIZER_MISMATCH (Con-ID: 0)
- Inst. ID: 1 Child Number: 2  OPTIMIZER_MISMATCH (Con-ID: 0)
- Inst. ID: 1 Child Number: 3  OPTIMIZER_MISMATCH (Con-ID: 0)
- Inst. ID: 1 Child Number: 4  OPTIMIZER_MISMATCH (Con-ID: 0)
- Inst. ID: 1 Child Number: 5  OPTIMIZER_MISMATCH (Con-ID: 0)
- Inst. ID: 1 Child Number: 6  OPTIMIZER_MISMATCH (Con-ID: 0)
...
```
The output will be automatically saved into the spool file with SQL_ID suffix in the name:
```
oracle@rkol7db1> ls -l sql_shared_cursor*
-rw-r--r-- 1 oracle oinstall 5586 27. Feb  22:03 sql_shared_cursor_8swypbbr0m372.log
oracle@rkol7db1> 
```

### Wait Event Parameters Description.

Get description of wait event parameters from V$EVENT_NAME:
```
./arept.py --get-wait-event 'buffer busy%'
Wait event: buffer busy waits ==> P1: file#;  P2: block#;  P3: class#;	Wait class: Concurrency
Wait event: buffer busy ==> P1: group#;  P2: obj#;  P3: block#;  Wait class: Other
```

### Resource Manager Plan.

Get Resource Manager Plan:

`arept.py --resource-plan DEFAULT_PLAN`

If you don't provide the plan name, then AREPT would display the current active plan. For the 
current plan AREPT would also generate the description of the default maintenance plan. 

For a multitenant database the default connection as SYSDBA would connect to CDB$ROOT and get 
current CDB plans for a multitenant database.

```
arept.py --resource-plan 
Output directory: /home/oracle/arept/arept_output
Current active resource plan: CDBE02_PLAN
Found following resource plans for maintenance windows
 - DEFAULT_MAINTENANCE_PLAN
 - File /home/oracle/arept/arept_output/cdb_resource_plan_CDBE02_PLAN_metadata.txt created.
 - File /home/oracle/arept/arept_output/cdb_resource_plan_DEFAULT_MAINTENANCE_PLAN_metadata.txt created.
```

Use *--pdb* option to get current resource plans for the PDB using the default connect as SYSDBA.

```
arept.py --resource-plan --pdb CDBE02_PDB1
Output directory: /home/oracle/arept/arept_output
Current active resource plan: CDBE02_PDB1_PLAN
Found following resource plans for maintenance windows
 - DEFAULT_MAINTENANCE_PLAN
 - File /home/oracle/arept/arept_output/resource_plan_CDBE02_PDB1_PLAN_metadata.txt created.
 - File /home/oracle/arept/arept_output/resource_plan_DEFAULT_MAINTENANCE_PLAN_metadata.txt created.
```

### Get Hidden Database Parameters.

You can use AREPT template option to generate a script for selecting both regular and hidden 
database parameters.  

```
arept.py -t hidden_parameters            

set echo off pagesi 100 linesi 256 trimsp on verify off
...

 - File /home/oracle/arept/arept_output/hidden_parameters.sql created.

```

This template generates both SQL statements for "Copy & Paste" and SQL script to get 
database parameters. The parameter names are selected using LIKE Where clause, so that
*'%'* meta characters are allowed.

If you only want to use the "Copy & Paste" output, then you should specify the parameter
name directly after the template option.

`arept.py -t hidden_parameters _%reserved%pct`

The generated output can be used to get the hidden database 
parameter *_shared_pool_reserved_pct*. You can of course use the generated SQL script
and provide the same parameter as a script parameter.
