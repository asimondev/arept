from __future__ import print_function
from __future__ import absolute_import

from .utils import file_created, get_instance_predicate


def print_template(name, out_dir, arept_args):
    t = Template(name=name, out_dir=out_dir, arept_args=arept_args)
    t.print()


class Template:
    def __init__(self, name, out_dir, arept_args):
        self.name = name
        self.out_dir = out_dir
        self.arept_args = arept_args
        self.header = "\n-- Created by arept (see https://github.com/asimondev/arept)\n\n"

        self.templates = {
            'process': self.print_process,
            'my_sql_trace': self.print_my_sql_trace,
            'ses_sql_trace': self.print_ses_sql_trace,
            'meta_table': self.print_meta_table,
            'meta_role': self.print_meta_role,
            'sql_details': self.sql_details,
            'awr_sql_monitor': self.awr_sql_monitor,
            'awr_sql_monitor_list': self.awr_sql_monitor_list,
            'sql_monitor_list': self.sql_monitor_list,
            'sql_monitor': self.sql_monitor,
            'sql_profile': self.sql_profile,
            'awr_sql_profile': self.awr_sql_profile,
            'get_awr_snap_ids': self.get_awr_snap_ids,
            'hidden_parameters': self.get_hidden_parameters,
            'sql_baseline': self.sql_plan_baseline,
            'awr_baseline': self.awr_sql_plan_baseline,
            'hinted_baseline': self.hinted_sql_plan_baseline,
            'get_sql_id': self.get_sql_id,
            'sql_shared_cursor': self.get_sql_shared_cursor,
            'check_sql_id': self.check_sql_id
        }

    def __str__(self):
        ret = "Class Template:\n"
        if self.name:
            ret += "- name: %s\n" % self.name
        if self.out_dir:
            ret += "- output directory: %s\n" % self.out_dir

        return ret

    def print(self):
        func = self.templates[self.name]
        func()

    def write_file(self, name, stmts):
        file_name = "%s/%s.sql" % (self.out_dir, name)
        with open(file_name, "w") as fout:
            fout.write(stmts)
        file_created(file_name)

    def print_process(self):
        # Default positional parameters SID Serial# Instance_ID
        (sid, ser, inst) = ("...", "...", "sys_context('userenv', 'instance')")
        if len(self.arept_args) >= 1:
            sid = self.arept_args[0]
        if len(self.arept_args) >= 2:
            ser = self.arept_args[1]
        if len(self.arept_args) >= 3:
            inst = self.arept_args[2]
        def_vars = """set echo on pagesi 100 linesi 256 trimsp on verify off

define my_sid=&1
define my_ser=&2
define my_inst=sys_context('userenv','instance')
-- define my_inst=&3
-- 
-- Set my_inst to 1 for single instance databases.
-- define my_inst=1

-- Uncomment this block, if you don't want to pass parameters to this script.
-- define my_sid=...
-- define my_ser=...
-- define my_inst=1 or sys_context('userenv','instance')
"""

        sel_stmt = """
select a.sid, a.serial# ser, a.inst_id inst, b.spid,
  a.con_id, a.username db_user, a.machine,
  b.program, b.tracefile, b.traceid
from gv$session a, gv$process b
where a.paddr = b.addr and a.con_id = b.con_id and a.inst_id = b.inst_id and 
"""

        stmts_out = sel_stmt + """  a.inst_id = %s and a.sid = %s and a.serial# = %s
/
""" % (inst, sid, ser)
        stmts_file = self.header + def_vars + sel_stmt + """  a.inst_id = &my_inst and 
  a.sid = &my_sid and a.serial# = &my_ser
/

select a.instance_number, a.instance_name, a.host_name, a.status
from gv$instance a where a.inst_id = &my_inst
/
"""
        print(stmts_out)
        self.write_file("process", stmts_file)

    def get_sql_id(self):
        def_vars = """
set echo on pagesi 100 linesi 256 trimsp on verify off

-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_text=&1

define my_sql_text=...
"""

        sel_stmt = """
select a.sql_id, a.child_number, a.executions execs, 
  a.plan_hash_value phv, a.full_plan_hash_value fphv, 
  a.inst_id, a.con_id, a.sql_text
from gv$sql a
where a.sql_text like '%&my_sql_text%'
-- and a.sql_id = ...
-- and a.inst_id = ...
/ 
"""

        stmts_out = sel_stmt
        stmts_file = self.header + def_vars + sel_stmt
        print(stmts_out)
        self.write_file("get_sql_id", stmts_file)

    def print_my_sql_trace(self):
        stmts = """
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
"""

        stmts_out = stmts
        stmts_file = self.header + stmts

        print(stmts_out)
        self.write_file("my_sql_trace", stmts_file)

    def print_ses_sql_trace(self):
        # Default positional parameters SID Serial# Instance_ID
        (sid, ser) = ("...", "...")
        if len(self.arept_args) >= 1:
            sid = self.arept_args[0]
        if len(self.arept_args) >= 2:
            ser = self.arept_args[1]

        def_vars = """
define my_sid=&1
define my_ser=&2

-- Uncomment this block, if you don't want to pass parameters to this script.
-- define my_sid=...
-- define my_ser=...
"""

        stmts = """        
set echo on pagesi 100 linesi 256 trimsp on verify on

spool arept_ses_sql_trace.log

select a.sid, a.serial# ser, b.spid,
  a.con_id, a.username db_user, a.machine,
  b.program, b.tracefile, b.traceid
from v$session a, v$process b
where a.paddr = b.addr and a.con_id = b.con_id and 
  a.sid = %s and a.serial# = %s
/

select a.instance_number, a.instance_name, a.host_name, a.status
from v$instance a
/
    
exec dbms_monitor.session_trace_enable(session_id=>%s, serial_num=>%s, waits=>true, binds=>false, plan_stat=>null);

spool off 

-- Run this command to finish the trace or disconnect the session.
-- exec dbms_monitor.session_trace_disable(session_id=>%s, serial_num=>%s);

"""

        stmts_out = stmts % (sid, ser, sid, ser, sid, ser)
        stmts_file = self.header + def_vars + stmts % ("&my_sid", "&my_ser",
                                            "&my_sid", "&my_ser",
                                            "&my_sid", "&my_ser") + """

-- Getting SQL trace using ORADEBUG:
-- oradebug setospid ...
-- oradebug event 10046 trace name context forever, level {1|4|8|12}
-- oradebug tracefile_name
-- ...
-- oradebug event 10046 trace name context off
"""

        print(stmts_out)
        self.write_file("ses_sql_trace", stmts_file)

    def print_meta_table(self):
        def_vars = """

-- Uncomment this block, if you want to pass parameters to this script.
-- define my_owner=&1
-- define my_table=&2

define my_owner=...
define my_table=...
"""

        stmts = """        
set echo on pagesi 100 linesi 256 trimsp on
set long 500000 longchunk 1000

set verify off
spool meta_table_%s_%s.log

select dbms_metadata.get_ddl(object_type=>'TABLE',name=>upper('%s'),schema=>upper('%s')) from dual
/

select dbms_metadata.get_dependent_ddl(object_type=>'INDEX',base_object_name=>upper('%s'),base_object_schema=>upper('%s')) from dual
/

select dbms_metadata.get_dependent_ddl(object_type=>'CONSTRAINT',base_object_name=>upper('%s'),base_object_schema=>upper('%s')) from dual
/

select count(*) from dba_triggers 
where table_name = upper('%s') and table_owner = upper('%s')
/

select round(sum(bytes)/(1024*1024), 0) mb from dba_segments 
where segment_name = upper('%s') and owner = upper('%s') and segment_type = 'TABLE'
/

select round(sum(bytes)/(1024*1024), 0) mb from dba_segments 
where segment_name = upper('%s') and owner = upper('%s') and segment_type = 'TABLE PARTITION'
/

select count(*) from dba_segments 
where segment_name = upper('%s') and owner = upper('%s') and segment_type = 'TABLE PARTITION'
/

select round(sum(bytes)/(1024*1024), 0) mb from dba_segments 
where segment_name = upper('%s') and owner = upper('%s') and segment_type = 'TABLE SUBPARTITION'
/

select count(*) from dba_segments 
where segment_name = upper('%s') and owner = upper('%s') and segment_type = 'TABLE SUBPARTITION'
/

spool off 
"""

        stmts_out = stmts % ("arept", "arept", "...", "...", "...", "...",
                             "...", "...", "...", "...", "...", "...",
                             "...", "...", "...", "...",
                             "...", "...", "...", "...")
        stmts_file = self.header + def_vars + stmts % ("&my_owner.", "&my_table.",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       "&my_table", "&my_owner",
                                                       )

        print(stmts_out)
        self.write_file("meta_table", stmts_file)

    def awr_sql_monitor(self):
        def_vars = """

-- Uncomment this block, if you want to pass parameters to this script.
-- define my_rid=&1
-- define my_type='&2'

define my_rid=...
-- Type can be 'XML', 'TEXT', 'HTML', 'EM' or 'ACTIVE'.
define my_type=...
"""

        stmts = """        
set echo on pagesi 10000 linesi 512 trimsp on trim on
set long 500000 longchunk 1000 heading off feedback off
set verify off echo off 

-- TEXT: col rep_output for a512

spool awr_sql_monitor.%s
select dbms_auto_report.report_repository_detail(rid=>&my_rid, type=>'&my_type') 
rep_output from dual
/
spool off 
"""

        stmts_out = stmts % "type"
        stmts_file = self.header + def_vars + stmts % "&my_type"

        print(stmts_out)
        self.write_file("awr_sql_monitor", stmts_file)

    def awr_sql_monitor_list(self):
        stmts = """
alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set echo on pagesi 100 linesi 256 trimsp on trim on

select report_id rid, key1, key2, period_start_time, period_end_time 
from dba_hist_reports 
where component_name = 'sqlmonitor' 
-- and key1 = sql_id 
-- and snap_id between ...
-- and period_start_time between ...
-- and period_end_time between ... 
order by key1, key2, period_start_time
/

select * from 
dba_hist_reports 
where component_name = 'sqlmonitor' 
-- and key1 = sql_id 
-- and snap_id between ...
-- and period_start_time between ...
-- and period_end_time between ...
order by key1, key2, period_start_time 
/
"""

        stmts_out = stmts
        stmts_file = self.header + stmts

        print(stmts_out)
        self.write_file("awr_sql_monitor_list", stmts_file)

    def sql_monitor_list(self):
        def_vars = """

-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_id='&1'
-- define my_type='&2'

define my_sql_id=...
-- Type can be 'xml', 'text', 'html' or 'active'.
define my_type=...
"""

        stmts = """
set echo off pagesi 1000 linesi 256 trimsp on trim on
set long 100000 longchunksize 1000000
set serveroutput on 

variable my_list clob
begin
  :my_list := dbms_sql_monitor.report_sql_monitor_list(sql_id => '%s',
    report_level => 'ALL', type => '%s');
end;
/
spool sql_monitor_list.%s
print :my_list
spool off
"""

        stmts_out = stmts % ("...", "...", "lst")
        stmts_file = self.header + def_vars + stmts % ("&my_sql_id",
                                                       "&my_type", "&my_type")

        print(stmts_out)
        self.write_file("sql_monitor_list", stmts_file)

    def sql_monitor(self):
        def_vars = """

-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_id='&1'
-- define my_type='&2'
-- define my_rep_level='&3'

define my_sql_id=...
-- Type can be 'xml', 'text', 'html' or 'active'.
define my_type=...
-- See available report levels at the end of the file. 
define my_rep_level='ALL'
"""

        stmts = """
-- Force monitoring with hin: /*+ monitor */
-- Force monitoring with event: ALTER SYSTEM SET EVENTS 'sql_monitor [sql: 5hc07qvt8v737|sql: 9ht3ba3arrzt3] force=true';

set echo off pagesi 1000 linesi 256 trimsp on trim on
set long 100000 longchunksize 1000000
set serveroutput on 

variable my_list clob
begin
  :my_list := dbms_sql_monitor.report_sql_monitor(sql_id => '%s',
    report_level => '%s', type => '%s');
end;
/
spool sql_monitor.%s
print :my_list
spool off
"""
        report_types = """

/*
Level of detail for the report. Of the following, only one can be specified:
NONE: Minimum possible
BASIC: This is equivalent to sql_text-plan-xplan-sessions-instance-activity_histogram-plan_histogram-metrics where the token "-" implies that report section will not be included in the report.
TYPICAL: Everything but plan_histogram
ALL: Everything

In addition, individual report sections can also be enabled or disabled by using a + or - section_name. Several sections are defined:
XPLAN: Shows explain plan. ON by default.
PLAN: Shows plan monitoring statistics. ON by default.
SESSIONS: Show session details. Applies only to parallel queries. ON by default.
INSTANCE: Shows instance details. Applies only to parallel and cross instance queries. ON by default.
PARALLEL: An umbrella parameter for specifying sessions as well as instance details
ACTIVITY: Shows activity summary at global level, plan line level and session
INSTANCE LEVEL: (If applicable). ON by default.
BINDS: Shows bind information when available. ON by default.
METRICS: Shows metric data (such as CPU and IOs) over time. ON by default
ACTIVITY_HISTOGRAM: Shows a histogram of the overall query activity. ON by default.
PLAN_HISTOGRAM: Shows activity histogram at plan line level. OFF by default.
OTHER: Other information. ON by default.

In addition, SQL text can be specified at different levels:

-SQL_TEXT: No SQL text in report
+SQL_TEXT: Alright with partial SQL text, that is, up to the first 2000 chars as stored in GV$SQL_MONITOR
SQL_FULLTEXT: No full SQL text, that is, +sql_text
+SQL_FULLTEXT: Show full SQL text (default)
*/
"""

        stmts_out = stmts % ("...", "ALL", "...", "lst")
        stmts_file = self.header + def_vars + stmts % ("&my_sql_id",
                "&my_rep_level", "&my_type", "&my_type") + report_types

        print(stmts_out)
        self.write_file("sql_monitor", stmts_file)

    def sql_details(self):
        def_vars = """

-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_id='&1'
-- define my_phv='&2'
-- define my_rep_level='&3'

define my_sql_id=...
define my_phv=NULL
-- See available report levels at the end of the file. 
define my_rep_level='ALL'
"""

        stmts = """
set echo off pagesi 1000 linesi 256 trimsp on trim on
set long 100000 longchunksize 1000000
set serveroutput on 
set feedback off heading off

variable my_clob clob
begin
  :my_clob := dbms_sqltune.report_sql_detail(sql_id => '%s',
    sql_plan_hash_value => %s,
    report_level => '%s');
end;
/
spool sql_details.html
print :my_clob
spool off
"""
        report_types = """

/*
Level of detail for the report, either 'BASIC', 'TYPICAL' or 'ALL'. Default assumes 'TYPICAL'. Their meanings are explained below.
In addition, individual report sections can also be enabled or disabled by using a +/- section_name. Several sections are defined:

'TOP'- Show top values for the ASH dimensions for a SQL statement; ON by default
'SPM'- Show existing plan baselines for a SQL statement; OFF by default
'MISMATCH'- Show reasons for creating new child cursors (sharing criteria violations); OFF by default.
'STATS'- Show SQL execution statistics per plan from GV$SQLAREA_PLAN_HASH; ON by default
'ACTIVITY' - Show top activity from ASH for each plan of a SQL statement; ON by default
'ACTIVITY_ALL' - Show top activity from ASH for each line of the plan for a SQL statement; OFF by default
'HISTOGRAM' - Show activity histogram for each plan of a SQL statement (plan time line histogram); ON by default
'SESSIONS' - Show activity for top sessions for each plan of a SQL statement; OFF by default
'MONITOR' - Show show one monitored SQL execution per execution plan; ON by default
'XPLAN' - Show execution plans; ON by default
'BINDS' - show captured bind data; ON by default

In addition, SQL text can be specified at different levels:

-SQL_TEXT - No SQL text in report
+SQL_TEXT - OK with partial SQL text up to the first 2000 chars as stored in GV$SQL_MONITOR
-SQL_FULLTEXT - No full SQL text (+SQL_TEXT)
+SQL_FULLTEXT - Show full SQL text (default value)

The meanings of the three top-level report levels are:

NONE - minimum possible
BASIC - SQL_TEXT+STATS+ACTIVITY+HISTOGRAM
TYPICAL - SQL_FULLTEXT+TOP+STATS+ACTIVITY+HISTOGRAM+XPLAN+MONITOR
ALL - everything

Only one of these 4 levels can be specified and, if it is, it has to be at the start of the REPORT_LEVEL string
*/
"""

        stmts_out = stmts % ("...", "NULL", "ALL")
        stmts_file = self.header + def_vars + stmts % ("&my_sql_id",
                "&my_phv", "&my_rep_level") + report_types

        print(stmts_out)
        self.write_file("sql_details", stmts_file)

    def sql_profile(self):
        create_task = """
-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_id='&1'
-- define my_task='&2'

define my_sql_id=...
define my_task='%s'

/*        
DBMS_SQLTUNE.CREATE_TUNING_TASK (
  sql_id           IN VARCHAR2,
  plan_hash_value  IN NUMBER    := NULL,
  scope            IN VARCHAR2  := SCOPE_COMPREHENSIVE or LIMITED
  time_limit       IN NUMBER    := TIME_LIMIT_DEFAULT in seconds 
  task_name        IN VARCHAR2  := NULL,
  description      IN VARCHAR2  := NULL,
  con_name         IN VARCHAR2  := NULL,
  database_link_to IN VARCHAR2  := NULL)
RETURN VARCHAR2;
*/

set echo on
set serveroutput on 

variable p_sql_id varchar2(128)
variable p_task varchar2(128)

begin
  :p_sql_id := '&my_sql_id';
  :p_task := '&my_task';
end;
/

print p_sql_id
print p_task

-- Create a new advisor task.

declare
  l_task_name varchar2(128);
begin
  l_task_name := dbms_sqltune.create_tuning_task(sql_id => :p_sql_id, 
    task_name => :p_task);
  dbms_output.put_line('Created new task: ' || l_task_name);
  :p_task := l_task_name;
end;
/

"""

        report_task = self.get_report_tuning_task()
        task_name = "AREPT_PROFILE_01"
        stmts = self.header + create_task % task_name
        self.write_file("start_sql_profile", stmts)

        stmts = self.header + report_task % task_name
        self.write_file("get_sql_profile", stmts)


    def awr_sql_profile(self):
        create_task = """
-- Uncomment this block, if you want to pass parameters to this script.
-- define my_begin_snap_id=&1
-- define my_end_snap_id=&2
-- define my_sql_id='&3'
-- define my_task='&4'

define my_begin_snap_id=...
define my_end_snap_id=...
define my_sql_id=...
define my_task='%s'

set echo on
set serveroutput on 

spool s01_start_task.log

variable p_begin_snap_id number
variable p_end_snap_id number
variable p_sql_id varchar2(128)
variable p_task varchar2(128)

begin
  :p_begin_snap_id := &my_begin_snap_id;
  :p_end_snap_id := &my_end_snap_id;
  :p_sql_id := '&my_sql_id';
  :p_task := '&my_task';
end;
/

print p_begin_snap_id
print p_end_snap_id
print p_sql_id
print p_task

-- Create a new advisor task.

/*        
DBMS_SQLTUNE.CREATE_TUNING_TASK (
  begin_snap       IN NUMBER,
  end_snap         IN NUMBER,
  sql_id           IN VARCHAR2,
  plan_hash_value  IN NUMBER    := NULL,
  scope            IN VARCHAR2  := SCOPE_COMPREHENSIVE,
  time_limit       IN NUMBER    := TIME_LIMIT_DEFAULT,
  task_name        IN VARCHAR2  := NULL,
  description      IN VARCHAR2  := NULL,
  con_name         IN VARCHAR2  := NULL,
  dbid             IN NUMBER    := NULL,
  database_link_to IN VARCHAR2  := NULL)
RETURN VARCHAR2;
*/


declare
  l_task_name varchar2(128);
begin
  l_task_name := dbms_sqltune.create_tuning_task(
    begin_snap => :p_begin_snap_id,
    end_snap => :p_end_snap_id,
    sql_id => :p_sql_id, 
    task_name => :p_task);
  dbms_output.put_line('Created a new task: ' || l_task_name);
  :p_task := l_task_name;
end;
/

"""
        report_task = self.get_report_tuning_task()
        task_name = "AREPT_AWR_PROFILE_01"
        stmts = self.header + create_task % task_name + \
                self.get_check_tuning_task() + "\nspool off\n"
        self.write_file("s01_start_task", stmts)

        stmts = self.header + report_task % task_name
        self.write_file("s02_get_report", stmts)

        stmts = self.header + self.check_tuning_task(task_name)
        self.write_file("check_task", stmts)

        print("\n => Use the s01_start_task.sql script to start the task. The "
              "script s03_get_report.sql should be used later to fetch the ready "
              "report. You can check the running task with the script check_task.sql.")
        print("\n => If you want to change the task name, don't forget to do it in "
              "all generated scripts. You can consider to adjust the default "
              "timeout value (600 seconds) for your task.")

    def get_check_tuning_task(self):
        stmts = """
-- Check advisor task status:

col task_id format 999999
col task_name format a25
col status_message format a33

select task_id, task_name, status, status_message
from   user_advisor_log 
where task_name = :p_task
/

-- Set parameters.

begin
  dbms_sqltune.set_tuning_task_parameter (
    task_name => :p_task,
    parameter => 'time_limit',
    value     => 600);
end;
/

-- Select current parameters:

col parameter_name format a25 
col value format a15   

select parameter_name, parameter_value as "value"
from   user_advisor_parameters
where  task_name = :p_task
and    parameter_value != 'UNUSED'
order by parameter_name
/

-- Start tuning task.

exec dbms_sqltune.execute_tuning_task(:p_task);

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

-- Check task progress.

select task_id, task_name, status, created, last_modified, execution_start, execution_end
from user_advisor_tasks where task_name = :p_task
/
        
"""
        return stmts

    def check_tuning_task(self, task_name):
        stmts = """
-- Use this script to check the status of advisor task.
-- You have to set the variable p_task before running this script.

-- Uncomment the line below, if you want to pass parameter to this script.
--define p_task='&1'
define p_task='%s'

col task_id format 999999
col task_name format a25
col status_message format a33

select task_id, task_name, status, status_message
from   user_advisor_log 
where task_name = '&p_task'
/

select task_id, task_name, status, created, last_modified, execution_start, execution_end
from user_advisor_tasks where task_name = :p_task
/

""" % task_name

        return stmts

    def get_report_tuning_task(self):
        stmts = """
define my_task='%s'

-- Show results, if the task was finished.

variable my_rep clob

begin
  :my_rep := dbms_sqltune.report_tuning_task(task_name => '&my_task', 
    type => 'TEXT', 
    level => 'ALL',  /* ALL or TYPICAL */ 
    section => 'ALL');
end;
/

set echo off pagesi 1000 linesi 256 trimsp on trim on
set long 500000 longchunksize 1000000
set serveroutput on

spool sql_profile.log
print :my_rep
spool off

-- Drop tuning task
-- exec dbms_sqltune.drop_tuning_task(task_name => '&my_task');
"""
        return stmts
    
    def get_awr_snap_ids(self):
        stmts = """set serveroutput on pagesi 100 linesi 256 trimsp on
set echo off

variable b_begin_snap_id number
variable b_end_snap_id number
variable b_dbid number

-- Set up your values for begin and end time as initial values for variables 
-- l_begin_time and l_end_time in the DECLARE block. 

declare 
    l_begin_time timestamp := to_timestamp('31-12-2022 23:59:59', 'dd-mm-yyyy hh24:mi:ss');
    l_end_time timestamp := to_timestamp('31-12-2022 23:59:59', 'dd-mm-yyyy hh24:mi:ss');
    l_min_snap_id number;
    l_max_snap_id number;
    l_dbid number;
begin
    select dbid into l_dbid from v$database;
    
    select max(snap_id) into l_min_snap_id from dba_hist_snapshot
    where dbid = l_dbid and begin_interval_time <= l_begin_time;

    select max(snap_id) into l_max_snap_id from dba_hist_snapshot
    where dbid = l_dbid and end_interval_time <= l_end_time;

    if l_min_snap_id is null or l_max_snap_id is null or l_min_snap_id >= l_max_snap_id then
        dbms_output.put_line('begin time: ' || l_begin_time ||
                     ', min_snap_id: ' || nvl(to_char(l_min_snap_id), 'NULL'));
        dbms_output.put_line('end time: ' || l_end_time ||
                     ', max_snap_id: ' || nvl(to_char(l_max_snap_id), 'NULL'));
        raise_application_error(-20001,
                        'Error: the specified AWR time interval is not available.');
    else
        dbms_output.put_line('found min snap_id: ' || l_min_snap_id);
        dbms_output.put_line('found max snap_id: ' || l_max_snap_id || ':');
    end if;
    
    :b_dbid := l_dbid;
    :b_begin_snap_id := l_min_snap_id;
    :b_end_snap_id := l_max_snap_id;
end;
/

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

set echo on linesi 80

col snap_id for 999999999
col inst for 99
col snap_begin for a20
col snap_end for a20

select snap_id, instance_number inst, begin_interval_time snap_begin, end_interval_time snap_end
from dba_hist_snapshot
where dbid = :b_dbid and snap_id in (:b_begin_snap_id, :b_end_snap_id)
order by snap_id, instance_number
/
"""
        stmts_file = self.header + stmts
        self.write_file("get_awr_snap_ids", stmts_file)

    def get_hidden_parameters(self):
        def_vars = """
set echo on pagesi 100 linesi 256 trimsp on verify off

-- MOS Doc ID 315631.1
-- 
-- Uncomment this block, if you want to pass parameters to this script.
-- define my_parameter=&1

define my_parameter=...
"""

        sel_stmt = """
SELECT a.ksppinm "Parameter", b.KSPPSTDF "Default Value",
       b.ksppstvl "Session Value", 
       c.ksppstvl "Instance Value",
       decode(bitand(a.ksppiflg/256,1),1,'TRUE','FALSE') IS_SESSION_MODIFIABLE,
       decode(bitand(a.ksppiflg/65536,3),1,'IMMEDIATE',2,'DEFERRED',3,'IMMEDIATE','FALSE') IS_SYSTEM_MODIFIABLE
FROM   x$ksppi a,
       x$ksppcv b,
       x$ksppsv c
WHERE  a.indx = b.indx
AND    a.indx = c.indx
-- a.ksppinm LIKE '/_clusterwide_global_transactions' escape '/'          
AND    a.ksppinm LIKE '&my_parameter'
/
        
"""

        stmts_out = sel_stmt
        stmts_file = self.header + def_vars + sel_stmt + """

-- For finding ISPDB_MODIFIABLE :

SELECT a.ksppinm "Parameter",
decode(bitand(ksppiflg/524288,1),1,'TRUE','FALSE') ISPDB_MODIFIABLE
FROM x$ksppi a
WHERE a.ksppinm LIKE '/_clusterwide_global_transactions' escape '/'          
/
"""
        print(stmts_out)
        self.write_file("hidden_parameters", stmts_file)

    def sql_plan_baseline(self):
        create_spm = """
-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_id='&1'
-- define my_phv=&2

define my_sql_id=...
define my_phv=...

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

/*
DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE (
   sql_id            IN  VARCHAR2,
   plan_hash_value   IN  NUMBER   := NULL,
   fixed             IN  VARCHAR2 := 'NO',
   enabled           IN  VARCHAR2 := 'YES')
RETURN PLS_INTEGER
*/

set echo on
set serveroutput on 

-- Load SQL execution plan using one of LOAD_PLANS_FROM_CURSOR_CACHE functions.

declare
  l_plans pls_integer;
begin
  l_plans := dbms_spm.load_plans_from_cursor_cache(sql_id => '&my_sql_id',
    plan_hash_value => &my_phv, 
    fixed => 'NO'); /* Use YES to load this plan as fixed. */
    
  dbms_output.put_line('Number of plans loaded: ' || l_plans);
end;
/

"""
        stmts = self.header + create_spm + self.select_spm_by_sqlid() + self.change_spm()
        self.write_file("create_sql_baseline", stmts)

    def select_spm_by_sqlid(self):
        stmts = """set verify off

select s.sql_id, s.plan_hash_value, b.sql_handle, b.plan_name, b.enabled, b.accepted, b.fixed, 
  b.origin, b.created, b.last_modified, b.last_executed, s.sql_text
from v$sql s JOIN dba_sql_plan_baselines b on 
  (s.exact_matching_signature = b.signature) and sql_id = '&my_sql_id'
/

"""
        return stmts

    def change_spm(self):
        stmts = """
-- Usage: change_baseline.sql after changing the PL/SQL code.

spool change_baseline.log
set echo on
set serveroutput on

-- Change / enable /disable created SPM:
/*
declare
  l_plans pls_integer;
begin
  l_plans := dbms_spm.alter_sql_plan_baseline(
    sql_handle => ...,
    plan_name => ..., 
--  attribute_name => 'enabled',  attribute_value => 'YES');
--  attribute_name => 'fixed', attribute_value => 'YES');
  dbms_output.put_line('Number of changed SPMs: ' || l_plans);
end;
/
*/
spool off
"""
        return stmts

    def set_hinted_spm(self):
        stmts = """
-- Uncomment this block, if you want to pass parameters to this script.
-- define my_hinted_sql_id='&1'
-- define my_hinted_phv=&2
-- define my_old_sql_handle='&3'

define my_hinted_sql_id=...
define my_hinted_phv=...
define my_old_sql_handle=...

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

set pagesi 100 linesi 256 trimsp on 
        
-- Add hints and execute the hinted SQL to get besser performance.

-- Find the SQL_ID and plan_hash_value from V$SQL or directly running this command after the SQL is successfully 
-- completed. Keep note of the SQL_ID and plan_hash_value for the hinted SQL , these will be used later.

-- Re-check the existing SQL plan baseline before adding the new plan:

select s.sql_id, b.sql_handle, b.plan_name, b.enabled, b.accepted, b.fixed, 
  b.origin, b.created, b.last_modified, b.last_executed, s.sql_text
from v$sql s JOIN dba_sql_plan_baselines b on 
  (s.exact_matching_signature = b.signature) and b.sql_handle = '&my_old_sql_handle'
/

-- Associate the hinted execution plan to the original sql_handle.

set serveroutput on
set echo on

declare
  l_plans pls_integer;
begin
  l_plans := dbms_spm.load_plans_from_cursor_cache(sql_id => '&my_hinted_sql_id', 
    plan_hash_value => &my_hinted_phv,
    sql_handle => '&my_old_sql_handle');
    
  dbms_output.put_line('Number of plans loaded: ' || l_plans);
end;
/

-- Verify the new baseline was added.

select s.sql_id, b.sql_handle, b.plan_name, b.enabled, b.accepted, b.fixed, 
  b.origin, b.created, b.last_modified, b.last_executed, s.sql_text
from v$sql s JOIN dba_sql_plan_baselines b on 
  (s.exact_matching_signature = b.signature) and b.sql_handle = '&my_old_sql_handle'
/

-- Check execution plans:

set pagesi 1000 linesi 256 trimsp on

select * from table(dbms_xplan.display_sql_plan_baseline(sql_handle=>'&my_old_sql_handle'))
/

/*
DBMS_XPLAN.DISPLAY_SQL_PLAN_BASELINE (
   sql_handle      IN VARCHAR2 := NULL,
   plan_name       IN VARCHAR2 := NULL,
   format          IN VARCHAR2 := 'TYPICAL')
 RETURN dbms_xplan_type_table;
*/

-- If the original plan captured initially is not needed, it can be dropped, or disabled.
/*
set serverout on
declare
  l_plans pls_integer;
begin
  l_plans :=dbms_spm.drop_sql_plan_baseline(sql_handle => ...,
     plan_name => ...);
  dbms_output.put_line('Number of plans dropped: ' || l_plans);
end;
/
*/

/*
DBMS_SPM.ALTER_SQL_PLAN_BASELINE (
   sql_handle        IN VARCHAR2 := NULL,
   plan_name         IN VARCHAR2 := NULL,
   attribute_name    IN VARCHAR2,
   attribute_value   IN VARCHAR2)
 RETURN PLS_INTEGER;

set serverout on
declare
  l_plans pls_integer;
begin
  l_plans :=dbms_spm.alter_sql_plan_baseline(sql_handle => ...,
     plan_name => ..., 
     attribute_name => 'enabled',
     attribute_value => 'NO');
--     attribute_name => 'fixed',
--     attribute_value => 'NO');
  dbms_output.put_line('Number of plans dropped: ' || l_plans);
end;
/
*/
"""
        return stmts

    def hinted_sql_plan_baseline(self):
        create_spm = """
-- Uncomment this block, if you want to pass parameters to this script.
-- define my_sql_id='&1'
-- define my_phv=&2

define my_sql_id=...
define my_phv=...

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

-- Based on Loading Hinted Execution Plans into SQL Plan Baseline. (MOS Doc ID 787692.1)

/*
DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE (
   sql_id            IN  VARCHAR2,
   plan_hash_value   IN  NUMBER   := NULL,
   fixed             IN  VARCHAR2 := 'NO',
   enabled           IN  VARCHAR2 := 'YES')
RETURN PLS_INTEGER
*/

set echo on
set serveroutput on 

-- Load SQL execution plan using one of LOAD_PLANS_FROM_CURSOR_CACHE functions.

declare
  l_plans pls_integer;
begin
  l_plans := dbms_spm.load_plans_from_cursor_cache(sql_id => '&my_sql_id',
    plan_hash_value => &my_phv, 
    fixed => 'NO'); /* Use YES to load this plan as fixed. */

  dbms_output.put_line('Number of plans loaded: ' || l_plans);
end;
/

"""
        stmts = self.header + create_spm + self.select_spm_by_sqlid()
        self.write_file("create_sql_baseline", stmts)

        stmts = self.header + self.set_hinted_spm()
        self.write_file("change_sql_baseline", stmts)

    def awr_sql_plan_baseline(self):
        create_spm = """
-- Usage: @create_awr_baseline SQL_ID Begin_Snap_ID End_Snap_ID         
define my_sql_id='&1'
define my_begin_snap=&2
define my_end_snap=&3

-- Uncomment this block, if you don't want to pass parameters to this script.
-- define my_sql_id=...
-- define my_begin_snap=...
-- define my_end_snap=...

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

/*
DBMS_SPM.LOAD_PLANS_FROM_AWR
   begin_snap      IN  NUMBER,
   end_snap        IN  NUMBER,
   basic_filter    IN  VARCHAR2 := NULL, -- Specifies filters defined on attributes of the SQLSET_ROW.
   fixed           IN  VARCHAR2 := 'NO',
   enabled         IN  VARCHAR2 := 'YES',
   commit_rows     IN  NUMBER := 1000)
 RETURN PLS_INTEGER;
*/

set echo on verify off
set serveroutput on 

spool create_awr_baseline_&my_sql_id..log

-- Load SQL execution plan(s) from AWR for the specified SQL_ID.

declare
  l_plans pls_integer;
begin
  l_plans := dbms_spm.load_plans_from_awr(
    begin_snap => &my_begin_snap,
    end_snap => &my_end_snap,
    basic_filter => 'sql_id = ''&my_sql_id''',
    fixed => 'NO'); /* Use YES to load this plan as fixed. */

  dbms_output.put_line('Number of plans loaded: ' || l_plans);
end;
/
spool off
"""
        stmts = self.header + create_spm
        self.write_file("create_awr_baseline", stmts)

        stmts = self.header + self.select_spm_last_hour()
        self.write_file("last_baselines", stmts)

        stmts = self.header + self.change_spm()
        self.write_file("change_baseline", stmts)

    def select_spm_last_hour(self):
        stmts = """
-- Usage: @last_baselines.sql

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool check_last_baselines.log
set echo on pagesi 100 linesi 100 trimsp on

-- SPMs created in last hour
select b.sql_handle, b.plan_name, b.enabled, b.accepted, b.fixed 
from dba_sql_plan_baselines b
where b.created > sysdate - 1/24
order by created 
/

select b.sql_handle, b.plan_name, b.origin, b.creator 
from dba_sql_plan_baselines b
where b.created > sysdate - 1/24
order by created 
/

select b.sql_handle, b.plan_name, b.created, b.last_modified, b.last_executed
from dba_sql_plan_baselines b
where b.created > sysdate - 1/24
order by created 
/

/*
select b.sql_handle, b.sql_text
from dba_sql_plan_baselines b
where b.sql_handle = ... 
  and rownum <= 1;
*/

spool off
"""
        return stmts

    def print_meta_role(self):
        # Default positional parameter: Role
        if len(self.arept_args) >= 1:
            role = self.arept_args[0]
            spool_file = "role_grants_%s.log" % role
        else:
            role = "..."
            spool_file = "role_grants.log"

        def_vars = """
define my_role=&1

-- Uncomment this block, if you don't want to pass parameters to this script.
-- define my_owner=...
"""

        stmts = """      
set echo on verify off 
set pagesi 1000 linesi 512 trimsp on
set long 100000 longhc 10000

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'PRETTY', TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', TRUE);

spool %s

SELECT dbms_metadata.get_ddl('ROLE', upper('%s')) from dual;
SELECT DBMS_METADATA.GET_GRANTED_DDL('SYSTEM_GRANT', upper('%s')) from dual;
SELECT DBMS_METADATA.GET_GRANTED_DDL('OBJECT_GRANT', upper('%s')) from dual;
SELECT DBMS_METADATA.GET_GRANTED_DDL('ROLE_GRANT',upper('%s')) from dual;

spool off     
"""

        stmts_out = stmts % (spool_file, role, role, role, role)
        stmts_file = self.header + def_vars + stmts % (spool_file, "&my_role",
                    "&my_role", "&my_role", "&my_role")

        print(stmts_out)
        self.write_file("meta_role", stmts_file)

    def get_sql_shared_cursor(self):
        def_vars = """set echo off pagesi 100 linesi 256 trimsp on verify off
set serveroutput on

-- Usage: @sql_shared_cursor SQL_ID [Instance_ID]

define my_sql_id=&1
-- Use my_inst in PL/SQL block and my_inst_select for SELECT statements.
define my_inst="sys_context(''userenv'',''instance'')"
define my_inst_select="sys_context('userenv','instance')"
-- Use my_inst in PL/SQL block and my_inst_select for SELECT statements.
--
-- define my_inst=&2
-- define my_inst=NULL -- for all instances
-- 
-- Set my_inst to 1 for single instance databases.
-- define my_inst=1

-- Uncomment this block, if you don't want to pass parameters to this script.
-- define my_sql_id=...
-- define my_inst=1 or sys_context('userenv','instance') or NULL
"""

        stmt = ("select a.* from gv$sql_shared_cursor a " +
                   "where a.inst_id = decode(&my_inst, NULL, a.inst_id, " +
                    "&my_inst) and a.sql_id = ''&my_sql_id'' " +
                    "order by inst_id, con_id, child_number")

        reason_stmt = ("select a.inst_id, a.child_number, a.con_id, a.reason " +
                       "from gv$sql_shared_cursor a where a.inst_id = " +
                       "decode(&my_inst, NULL, a.inst_id, &my_inst) and " +
                       "a.sql_id = ''&my_sql_id'' order by inst_id, con_id, child_number")

        pls = self.sql_shared_cursor_plsql(stmt, reason_stmt)
        stmts_file = self.header + def_vars + pls
        self.write_file("sql_shared_cursor", stmts_file)

    def sql_shared_cursor_plsql(self, stmt, reason_stmt):
        stmts = """spool sql_shared_cursor_&my_sql_id..log
        
declare
  l_sql_id varchar2(128) := '&my_sql_id';
  l_crs integer := dbms_sql.open_cursor;
  l_stmt varchar2(2048) := '%s';
  l_col_count pls_integer;
  l_col_desc dbms_sql.desc_tab;
  l_col_value varchar2(32767);
  l_ret pls_integer;
  l_res varchar2(32767);
  l_col_name varchar2(128);
  l_first boolean; 
begin
  dbms_sql.parse(l_crs, l_stmt, dbms_sql.native);
  dbms_sql.describe_columns(l_crs, l_col_count, l_col_desc);
  
  for i in 1..l_col_count loop
    dbms_sql.define_column(l_crs, i, l_col_value, 32767);   
  end loop;
  
  l_ret := dbms_sql.execute(l_crs);

  dbms_output.put_line('Output from GV$SQL_SHARED_CURSOR:');
  while dbms_sql.fetch_rows(l_crs) > 0 loop
    l_res := '';
    l_first := true;
    for i in 1..l_col_count loop
        dbms_sql.column_value(l_crs, i, l_col_value);
        l_col_name := l_col_desc(i).col_name;

        if l_col_name = 'INST_ID' then
          l_res := l_res || '- Inst. ID: ' || l_col_value;
        elsif l_col_name = 'CHILD_NUMBER' then
          l_res := l_res || ' Child Number: ' || l_col_value || ' ';
        elsif l_col_name = 'CON_ID' then
          l_res := l_res || ' (Con-ID: ' || l_col_value || ')';
        end if;

        if l_col_value = 'Y' and l_col_desc(i).col_type = 1 and 
               l_col_desc(i).col_max_len = 1 then
          if l_first then
            l_first := false;
          else
            l_res := l_res || ';';
          end if;
          
          l_res := l_res || ' ' || l_col_name;
        end if;
    end loop;
    dbms_output.put_line(l_res);
  end loop;

  dbms_sql.close_cursor(l_crs);
end;
/

declare
  l_stmt varchar2(2048) := '%s';
  l_crs sys_refcursor;
  l_inst_id pls_integer;
  l_child_number pls_integer;
  l_con_id pls_integer;
  l_reason varchar2(32767);
begin
  open l_crs for l_stmt;
    
  dbms_output.put_line('  ');
  dbms_output.put_line('Reasons from GV$SQL_SHARED_CURSOR:');
  dbms_output.put_line('  ');
    
  loop
    fetch l_crs into l_inst_id, l_child_number, l_con_id, l_reason;
    exit when l_crs%%notfound;
    dbms_output.put_line('- Inst. ID: ' || l_inst_id || ' Child Number: ' ||
        l_child_number || ' (Con-ID: ' || l_con_id || ')');
    dbms_output.put_line('---> Reason: ' || l_reason);
    dbms_output.put_line('  ');
  end loop;
  close l_crs;
end;
/


alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set echo on linesi 100 trimsp on

col inst_id for 9999999
col con_id for 99999
col child for 99999
col execs for 999999
col first_load_time for a20
col last_load_time for a20
col last_active_time for a20

select a.inst_id, a.con_id, a.child_number child, a.executions execs,
 a.first_load_time, a.last_load_time, a.last_active_time
from gv$sql a 
where a.sql_id = '&my_sql_id' and 
  a.inst_id = decode(&my_inst_select, NULL, a.inst_id, &my_inst_select)
order by inst_id, con_id, child_number
/

set long 1000000 longch 1000 pagesi 1000 linesi 256 trimsp on

select sql_fulltext from gv$sql where sql_id = '&my_sql_id' and rownum <= 1;

spool off
""" % (stmt, reason_stmt)

        return stmts

    def check_sql_id(self):
        # Default positional parameters SQL_ID Instance
        (sql_id, inst) = ("...", "sys_context('userenv', 'instance')")
        if len(self.arept_args) >= 1:
            sql_id = self.arept_args[0]
        if len(self.arept_args) >= 2:
            inst = self.arept_args[1]
        def_vars = """set echo on pagesi 100 linesi 256 trimsp on verify off

define my_sql_id=&1
define my_inst=sys_context('userenv','instance')
-- define my_inst=&2
-- Set my_inst to NULL for all instances.
-- 
-- Set my_inst to 1 for single instance databases.
-- define my_inst=1

-- Uncomment this block, if you don't want to pass parameters to this script.
-- define my_sql_id=...
-- define my_inst=1 or sys_context('userenv','instance')

spool check_sql_id_&my_sql_id..log
"""

        sel_stmt = """
select distinct parsing_schema_name, service, module, action 
from gv$sql where sql_id = '%s'
/
"""
        stmts_out = sel_stmt % sql_id
        stmts_file = sel_stmt % "&my_sql_id"

        sel_stmt = """
select inst_id, sql_id, child_number, con_id, executions execs
from gv$sql
where """

        stmts_out += sel_stmt + """sql_id = '%s' and inst_id = %s
/
""" % (sql_id, get_instance_predicate("inst_id", inst))
        stmts_file += self.header + def_vars + sel_stmt + """sql_id = '&my_sql_id' and 
inst_id = %s
/

select a.instance_number, a.instance_name, a.host_name, a.status
from gv$instance a
order by inst_id
/

spool off
""" % get_instance_predicate("inst_id", "&my_inst")

        print(stmts_out)
        self.write_file("check_sql_id", stmts_file)
