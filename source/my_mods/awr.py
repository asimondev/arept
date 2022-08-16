from __future__ import print_function
from __future__ import absolute_import

from .sqlplus import SqlPlus
from .utils import file_created, desc_stmt

class AWRSQLNotFound(Exception): pass


def print_awr_sql(sql_id=None, begin_id=None, end_id=None,
                  params={}, verbose=False):
    sql = AWRSQL(sql_id=sql_id, begin_id=begin_id, end_id=end_id,
                 params=params, verbose=verbose)
    if verbose:
        print(sql)

    if params['is_rac']:
        for inst_id in params['rac_inst_ids']:
            if "text" in params['out_format']:
                sql.print_text_report(inst_id)
            if "html" in params['out_format']:
                sql.print_html_report(inst_id)
    else:
        if "text" in params['out_format']:
            sql.print_text_report(params['inst_id'])
        if "html" in params['out_format']:
            sql.print_html_report(params['inst_id'])

    sql.get_mon_list()


class AWRSQL:
    def __init__(self,
                sql_id=None,
                begin_id=None,
                end_id=None,
                params={},
                verbose=False):
        self.sql_id = sql_id
        self.begin_id = begin_id
        self.end_id = end_id
        self.params = params
        self.verbose = verbose

    def __str__(self):
        ret = "Class AWRSQL:\n"
        if self.sql_id:
            ret += "- SQL_ID: %s\n" % self.sql_id
        if self.begin_id:
            ret += "- begin snapshot ID: %s\n" % self.begin_id
        if self.end_id:
            ret += "- end snapshot ID: %s\n" % self.end_id

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
        if self.params['version']:
            ret += "- database version: %s\n" % self.params['version']
        if self.params['awr_sql_format']:
            ret += "- AWR SQL format options: %s\n" % self.params['awr_sql_format']

        return ret

    def print_text_report(self, inst_id):
        file_name = "awr_sql_id_%s_inst_%s_report.txt" % (self.sql_id, inst_id)
        stmts = """set echo on pagesi 1000 linesi 256 trimsp on long 50000 longchunk 1000

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s/%s

select output from table(
  dbms_workload_repository.awr_sql_report_text(
  l_bid=>%s,l_eid=>%s,l_sqlid=>'%s',l_dbid=>%s,l_inst_num=>%s));

spool off
""" % (self.params['out_dir'], file_name, self.begin_id, self.end_id, self.sql_id,
       self.params['dbid'], inst_id)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False, do_exit=False)
        file_created(file_name)
        if self.verbose:
            for line in out:
                print(line)
        # ret = []
        # for line in out:
        #     print(line)
        # matchobj = re.match(r'\ATABLE:([\w]+)\s*\Z', line)
        # if matchobj:
        #     ret.append(matchobj.group(1))
        #     break

        # return ret

    def print_html_report(self, inst_id):
        file_name = "awr_sql_id_%s_inst_%s_report.html" % (self.sql_id, inst_id)
        stmts = """set echo off feedback off verify off termout on; 
set pagesi 0 linesi 8000 trimsp on long 50000 longchunk 1000;
set heading off 
set serveroutput ON SIZE UNLIMITED FORMAT WRAPPED

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s/%s
select output from table(dbms_workload_repository.awr_sql_report_html(l_bid=>%s,l_eid=>%s,l_sqlid=>'%s',l_dbid=>%s,l_inst_num=>%s));
spool off
""" % (self.params['out_dir'], file_name, self.begin_id, self.end_id, self.sql_id,
       self.params['dbid'], inst_id)

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

    def get_mon_list(self):
        for fmt in self.params['out_format']:
            name = "awr_sql_id_%s_hist_reports" % self.sql_id
            if fmt == "text":
                file_name = name + ".txt"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on long 50000 echo on
"""
            else:
                file_name = name + ".html"
                fmt_stmts = """
set pagesi 100 linesi 256 trimsp on echo on
set markup html on spool on 
"""

            stmts = """
spool %s/%s

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';

%s 

select report_id rid, key1, key2, period_start_time, period_end_time 
from dba_hist_reports 
where key1 = '%s' and component_name = 'sqlmonitor' and snap_id between %s and %s 
order by period_start_time
/

select * from dba_hist_reports where key1 = '%s' and component_name = 'sqlmonitor' 
and snap_id between %s and %s order by period_start_time
/

spool off
""" % (self.params['out_dir'], file_name,
       desc_stmt("dba_hist_reports"),
       self.sql_id, self.begin_id, self.end_id,
       self.sql_id, self.begin_id, self.end_id)

            sql = SqlPlus(con=self.params['db_con'],
                          pdb=self.params['pdb'],
                          stmts=fmt_stmts + stmts,
                          out_dir=self.params['out_dir'],
                          verbose=self.verbose)
            out = sql.run(silent=True)
            file_created(file_name)
            if self.verbose:
                for line in out:
                    print(line)
