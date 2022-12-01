from __future__ import print_function
from __future__ import absolute_import

from .sqlplus import SqlPlus
from .utils import file_created, desc_stmt


class SQLNotFound(Exception): pass


def print_sql(sql_id=None, sql_child=None,
                  params=None, verbose=False):
    sql = SQL(sql_id=sql_id, sql_child=sql_child,
                 params=params, verbose=verbose)
    if verbose:
        print(sql)

    child = sql.get_child()
    sql.display_cursor(child)
    sql.select_sql(child)
    sql.select_sqlarea(child)

    for fmt in sql.params['out_format']:
        if fmt == "text":
            sql.get_mon_list_text(child)
            sql.get_mon_rep_text(child)
        if fmt == "html":
            sql.get_sql_details(child)
            sql.get_mon_list_html(child, "html")
            sql.get_mon_list_html(child, "active")
            sql.get_mon_rep_html(child, "html")
            sql.get_mon_rep_html(child, "active")


class SQL:
    def __init__(self,
                sql_id=None,
                sql_child=None,
                params=None,
                verbose=False):
        self.sql_id = sql_id
        self.sql_child = sql_child
        self.params = params
        self.verbose = verbose

        self.child = sql_child if sql_child else 0
        self.fmt = params['sql_format'] if params.get('sql_format') else 'TYPICAL'

    def __str__(self):
        ret = "Class SQL:\n"
        if self.sql_id:
            ret += "- SQL_ID: %s\n" % self.sql_id
        if self.sql_child:
            ret += "- SQL child number: %d\n" % self.sql_id
        if self.fmt:
            ret += "- SQL format option: %s\n" % self.fmt

        if self.params['db_con']:
            ret += "- SQL*Plus connection string: %s\n" % self.params['db_con']
        if self.params['pdb']:
            ret += "- PDB: %s\n" % self.params['pdb']
        if self.params['dbid']:
            ret += "- DBID: %s\n" % self.params['dbid']
        if self.params['inst_name']:
            ret += "- Instance name: %s\n" % self.params['inst_name']
        if self.params['inst_id']:
            ret += "- Instance ID: %s\n" % self.params['inst_id']
        if self.params['is_rac'] is not None:
            ret += "- RAC? %s\n" % self.params['is_rac']
        if self.params['is_rac']:
            ret += "- RAC instance IDs: %s\n" % self.params['rac_inst_ids']
        if self.params['is_dba'] is not None:
            ret += "- DBA? %s\n" % self.params['is_dba']
        if self.params['out_dir']:
            ret += "- output directory: %s\n" % self.params['out_dir']
        if self.params['out_format']:
            ret += "- output format: %s\n" % self.params['out_format']
        if self.params['out_level']:
            ret += "- output level: %s\n" % self.params['out_level']
        if self.params['sql_format']:
            ret += "- SQL format option: %s\n" % self.params['sql_format']
        if self.params['version']:
            ret += "- database version: %s\n" % self.params['version']
        if self.params['major_version']:
            ret += "- database major version: %s\n" % self.params['major_version']

        return ret

    def get_child(self):
        if self.sql_child:
            return "_child_%d" % self.sql_child

        return ""

    def display_cursor(self, child):
        file_name = "%s/sql_id_%s%s_xplan.txt" % (
            self.params['out_dir'], self.sql_id, child)

        stmts = """set pagesi 5000 linesi 256 trimsp on long 50000 echo on

spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

select * from table(
  dbms_xplan.display_cursor(sql_id => '%s', cursor_child_no => %d, format => '%s'));
  
spool off
""" % (file_name, self.sql_id, self.child, self.fmt)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=True, do_exit=False)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)

    def select_sql(self, child):
        if self.sql_child is None:
            child_number = ""
        else:
            child_number = " and child_number = %d" % self.sql_child

        for fmt in self.params['out_format']:
            name = "%s/sql_id_%s%s_sql" % (self.params['out_dir'],
                                           self.sql_id, child)
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + ".html"
                fmt_stmts = """
set pagesi 1000 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

select sql_id, child_number, plan_hash_value phv, full_plan_hash_value fphv,
  executions execs, buffer_gets bgets
from v$sql where sql_id = '%s' %s 
order by child_number
/

%s

%s

select * from v$sql where sql_id = '%s' %s
order by child_number
/

select sql_fulltext from v$sql where sql_id = '%s' %s and rownum < 2 
/

spool off
""" % (file_name,
       self.sql_id, child_number, self.select_sql_quarantine(child_number),
       desc_stmt("v$sql"),
       self.sql_id, child_number,
       self.sql_id, child_number)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name)
            if self.verbose:
                for line in out:
                    print(line)

    def select_sql_quarantine(self, child):
        if self.params['major_version'] == 19:
            stmt = """
select sql_id, child_number, executions exec, plan_hash_value phv, full_plan_hash_value fphv,
  sql_profile sprof, sql_patch spatch, sql_plan_baseline sbase, sql_quarantine squar 
from v$sql where sql_id = '%s' %s and
  sql_profile is not null or sql_patch is not null or sql_plan_baseline is not null 
  or sql_quarantine is not null
order by child_number
/
"""
        else:
            stmt = """
select sql_id, child_number, executions exec, plan_hash_value phv, full_plan_hash_value fphv,
  sql_profile sprof, sql_patch spatch, sql_plan_baseline sbase 
from v$sql where sql_id = '%s' %s and
  sql_profile is not null or sql_patch is not null or sql_plan_baseline is not null 
order by child_number
/
"""
        return stmt % (self.sql_id, child)

    def select_sqlarea(self, child):
        for fmt in self.params['out_format']:
            name = "%s/sql_id_%s%s_sqlarea" % (self.params['out_dir'],
                                               self.sql_id, child)
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + ".html"
                fmt_stmts = """
set pagesi 100 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

select sql_id, version_count, loaded_versions, first_load_time 
from v$sqlarea where sql_id = '%s' 
/

%s

select * from v$sqlarea where sql_id = '%s'
/

select sql_fulltext from v$sqlarea where sql_id = '%s' 
/

spool off
""" % (file_name,
       self.sql_id,
       desc_stmt("v$sqlarea"),
       self.sql_id,
       self.sql_id)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=False)
            file_created(file_name)
            if self.verbose:
                for line in out:
                    print(line)

    def get_mon_list_text(self, child):
        file_name = "%s/sql_id_%s%s_mon_list.txt" % (self.params['out_dir'],
                                                     self.sql_id, child)
        stmts = """             
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set pagesi 1000 linesi 1000 trimsp on trim on
set long 100000 longchunksize 1000000 echo on trim on
set serveroutput on 

variable my_list clob
begin
  :my_list := dbms_sql_monitor.report_sql_monitor_list(sql_id => '%s',
    report_level => 'ALL', TYPE => 'text');
end;
/
print :my_list

spool off
""" % (file_name, self.sql_id)\

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)

    def get_mon_rep_text(self, child):
        file_name = "%s/sql_id_%s%s_mon_report.txt" % (self.params['out_dir'],
                                                       self.sql_id, child)
        stmts = """             
spool %s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set pagesi 1000 linesi 1000 trimsp on trim on
set long 100000 longchunksize 1000000 echo on trim on
set serveroutput on 

variable my_rep clob
begin
  :my_rep := dbms_sql_monitor.report_sql_monitor(sql_id => '%s',
    report_level => 'ALL', TYPE => 'text');
end;
/
print :my_rep

spool off
""" % (file_name, self.sql_id)\

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)

    def get_mon_list_html(self, child, list_type):
        name = "%s/sql_id_%s%s_mon_list" % (self.params['out_dir'],
                                            self.sql_id, child)
        if list_type == "html":
            file_name = name + ".html"
        else:
            file_name = name + "_active.html"
        stmts = """             
alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set pagesi 0 linesi 32768 trimsp on trim on
set long 100000 longchunksize 1000000
set serveroutput on
set feedback off termout off heading off

column text_line format a254

spool %s
select dbms_sql_monitor.report_sql_monitor_list(sql_id => '%s',
    report_level => 'ALL', TYPE => '%s') text_line from dual;
spool off
""" % (file_name, self.sql_id, list_type.upper())

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=True)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)


    def get_mon_rep_html(self, child, list_type):
        name = "%s/sql_id_%s%s_mon_report" % (self.params['out_dir'],
                                              self.sql_id, child)
        if list_type == "html":
            file_name = name + ".html"
        else:
            file_name = name + "_active.html"
        stmts = """             
alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set pagesi 0 linesi 1000 trimsp on trim on
set long 100000 longchunksize 1000000
set serveroutput on 
set feedback off termout off

spool %s

select dbms_sql_monitor.report_sql_monitor(sql_id => '%s',
    report_level => 'ALL', TYPE => '%s') from dual;

spool off
""" % (file_name, self.sql_id, list_type)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=True)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)

    def get_sql_details(self, child):
        name = "%s/sql_id_%s%s_detail" % (self.params['out_dir'],
                                          self.sql_id, child)
        file_name = name + "_active.html"
        stmts = """             
alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

set pagesi 0 linesi 1000 trimsp on trim on
set long 100000 longchunksize 1000000
set serveroutput on 
set feedback off termout off

spool %s

select dbms_sqltune.report_sql_detail(sql_id => '%s',
    report_level => 'ALL') from dual;

spool off
""" % (file_name, self.sql_id)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=True)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)
