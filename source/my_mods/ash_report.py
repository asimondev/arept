from __future__ import print_function
from __future__ import absolute_import

from time import strftime

from .sqlplus import SqlPlus
from .utils import file_created, date_to_str


def generate_ash_report(begin_time, end_time, params,
                        verbose, global_ash_report):
    ash = ASHReport(begin_time, end_time, params,
                    verbose, global_ash_report)
    if verbose:
        print(ash)

    if global_ash_report:
        ash.print_global_report()
    else:
        if "text" in params['out_format']:
            ash.print_text_report(params['inst_id'])
        if "html" in params['out_format']:
            ash.print_html_report(params['inst_id'])


class ASHReport:
    def __init__(self, begin_time, end_time, params,
                 verbose, global_ash_report):
        self.begin_time = begin_time
        self.end_time = end_time
        self.params = params
        self.verbose = verbose
        self.global_ash_report = global_ash_report

        self.db_id = params['dbid']
        self.inst_name = params['inst_name']
        self.out_dir = params['out_dir']
        self.formats = params['out_format']
        self.ash_dir = "ash_reports_%s" % strftime("%Y-%m-%d_%H-%M-%S")
        self.parallel = params['parallel']

    def __str__(self):
        ret = "Class ASHReport:\n"
        ret += "- begin time: %s\n" % self.begin_time
        ret += "- end time: %s\n" % self.end_time
        ret += "- db_id: %s\n" % self.db_id
        ret += "- inst_name: %s\n" % self.inst_name
        ret += "- out_dir: %s\n" % self.out_dir
        ret += "- ash_dir: %s\n" % self.ash_dir
        ret += "- formats: %s\n" % ','.join(self.formats)
        if self.parallel is not None:
            ret += "- parallel: %s\n" % self.parallel

        return ret

    # DBMS_WORKLOAD_REPOSITORY.ASH_REPORT_TEXT(
    #    l_dbid          IN NUMBER,
    #    l_inst_num      IN NUMBER,
    #    l_btime         IN DATE,
    #    l_etime         IN DATE,
    #    l_options       IN NUMBER    DEFAULT 0,
    #    l_slot_width    IN NUMBER    DEFAULT 0,
    #    l_sid           IN NUMBER    DEFAULT NULL,
    #    l_sql_id        IN VARCHAR2  DEFAULT NULL,
    #    l_wait_class    IN VARCHAR2  DEFAULT NULL,
    #    l_service_hash  IN NUMBER    DEFAULT NULL,
    #    l_module        IN VARCHAR2  DEFAULT NULL,
    #    l_action        IN VARCHAR2  DEFAULT NULL,
    #    l_client_id     IN VARCHAR2  DEFAULT NULL,
    #    l_plsql_entry   IN VARCHAR2  DEFAULT NULL,
    #    l_data_src      IN NUMBER    DEFAULT 0,
    #    l_container     IN VARCHAR2  DEFAULT NULL)
    #  RETURN awrrpt_text_type_table PIPELINED;

    def print_text_report(self, inst_id):
        file_name = "ash_report_inst_%s_%s.txt" % \
                    (inst_id, date_to_str(self.begin_time))

        stmts = """set echo on pagesi 1000 linesi 256 trimsp on 
set long 50000 longchunk 1000

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

col output for a80

set echo off heading off feedback off pagesi 0
spool %s/%s

select output from table(
  dbms_workload_repository.ash_report_text(l_dbid => %s,
    l_inst_num => %s, 
    l_btime => to_date('%s', 'yyyy-mm-dd hh24:mi'),
    l_etime => to_date('%s', 'yyyy-mm-dd hh24:mi')
  ));

spool off
""" % (self.params['out_dir'], file_name, self.db_id, inst_id,
       self.begin_time, self.end_time)

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

# DBMS_WORKLOAD_REPOSITORY.ASH_REPORT_HTML(
#    l_dbid          IN NUMBER,
#    l_inst_num      IN NUMBER,
#    l_btime         IN DATE,
#    l_etime         IN DATE,
#    l_options       IN NUMBER    DEFAULT 0,
#    l_slot_width    IN NUMBER    DEFAULT 0,
#    l_sid           IN NUMBER    DEFAULT NULL,
#    l_sql_id        IN VARCHAR2  DEFAULT NULL,
#    l_wait_class    IN VARCHAR2  DEFAULT NULL,
#    l_service_hash  IN NUMBER    DEFAULT NULL,
#    l_module        IN VARCHAR2  DEFAULT NULL,
#    l_action        IN VARCHAR2  DEFAULT NULL,
#    l_client_id     IN VARCHAR2  DEFAULT NULL,
#    l_plsql_entry   IN VARCHAR2  DEFAULT NULL,
#    l_data_src      IN NUMBER    DEFAULT 0,
#    l_container     IN VARCHAR2  DEFAULT NULL)
#  RETURN awrrpt_html_type_table PIPELINED;

    def print_html_report(self, inst_id):
        file_name = "ash_report_inst_%s_%s.html" % \
                    (inst_id, date_to_str(self.begin_time))

        stmts = """set echo off pagesi 0 
set linesi 8000 trimsp on 
set long 500000 longchunk 1000
set heading off feedback off

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';


spool %s/%s

select output from table(
  dbms_workload_repository.ash_report_html(l_dbid => %s,
    l_inst_num => %s, 
    l_btime => to_date('%s', 'yyyy-mm-dd hh24:mi'),
    l_etime => to_date('%s', 'yyyy-mm-dd hh24:mi')
  ));

spool off
""" % (self.params['out_dir'], file_name, self.db_id, inst_id,
       self.begin_time, self.end_time)

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

    def print_global_report(self):
        for fmt in self.params['out_format']:
            name = "global_ash_report_%s" % date_to_str(self.begin_time)

            if fmt == "text":
                file_name = name + ".txt"

                stmts = """set echo off pagesi 0 
set linesi 80 trimsp on 
set long 500000 longchunk 1000
set heading off feedback off

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s/%s

select output from table(
  dbms_workload_repository.ash_global_report_text(l_dbid => %s,
    l_inst_num => NULL,
    l_btime => to_date('%s', 'yyyy-mm-dd hh24:mi'),
    l_etime => to_date('%s', 'yyyy-mm-dd hh24:mi')
  ));

spool off
""" % (self.params['out_dir'], file_name, self.db_id,
       self.begin_time, self.end_time)
            else:
                file_name = name + ".html"

                stmts = """set echo off pagesi 0 
set linesi 8000 trimsp on 
set long 500000 longchunk 1000
set heading off feedback off

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s/%s

select output from table(
  dbms_workload_repository.ash_global_report_html(l_dbid => %s,
    l_inst_num => NULL,
    l_btime => to_date('%s', 'yyyy-mm-dd hh24:mi'),
    l_etime => to_date('%s', 'yyyy-mm-dd hh24:mi')
  ));

spool off
""" % (self.params['out_dir'], file_name, self.db_id,
       self.begin_time, self.end_time)

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
