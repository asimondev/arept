from __future__ import print_function
from __future__ import absolute_import

import os
from time import strftime

from .utils import file_created
from .sqlplus import SqlPlus

def generate_perfhub_report(params, verbose,
                            begin_time=None, end_time=None,
                            rt_perfhub_report=False,
                            awr_perfhub_report=False):
    ph = PerfHubReport(params=params, verbose=verbose,
                       begin_time=begin_time, end_time=end_time,
                       rt_perfhub_report=rt_perfhub_report,
                       awr_perfhub_report=awr_perfhub_report)
    if verbose:
        print(ph)

    ph.generate_report()


class PerfHubReport:
    def __init__(self,
                 params=None,
                 verbose=False,
                 begin_time=None,
                 end_time=None,
                 rt_perfhub_report=False,
                 awr_perfhub_report=False):
        self.params = params
        self.verbose = verbose
        self.begin_time = begin_time
        self.end_time = end_time
        self.rt_perfhub_report = rt_perfhub_report
        self.awr_perfhub_report = awr_perfhub_report

        self.db_id = params['dbid']
        self.inst_name = params['inst_name']
        self.out_dir = params['out_dir']
        self.formats = params['out_format']

        self.ph_dir = "perfhub_reports_%s" % strftime("%Y-%m-%d_%H-%M-%S")
        self.parallel = params['parallel']

    def __str__(self):
        ret = "Class PerfHubReport:\n"
        ret += "- db_id: %s\n" % self.db_id
        ret += "- inst_name: %s\n" % self.inst_name
        ret += "- out_dir: %s\n" % self.out_dir
        ret += "- ph_dir: %s\n" % self.ph_dir
        ret += "- formats: %s\n" % ','.join(self.formats)
        if self.parallel is not None:
            ret += "- parallel: %s\n" % self.parallel
        ret += "- real-time performance hub report: %s\n" % self.rt_perfhub_report
        ret += "- AWR performance hub report: %s\n" % self.awr_perfhub_report

        return ret

    # DBMS_PERF.REPORT_PERFHUB (
    #    If 1, then real-time. If NULL (default) or 0, then historical mode.
    #    is_realtime          IN NUMBER   DEFAULT NULL,
    # Start time of outer period shown in the time selector. If NULL (default):
    #  - If is_realtime=0 (historical), then 24 hours before outer_end_time.
    #  - If is_realtime=1 (realtime mode), then 1 hour before outer_end_time.
    #    outer_start_time     IN DATE     DEFAULT NULL,
    # End time of outer period shown in the time selector. If NULL (default), then latest AWR snapshot.
    #  - If is_realtime=0 (historical), then the latest AWR snapshot
    #  - If is_realtime=1 (realtime mode), this is the current time (and any input is ignored)
    #    outer_end_time       IN DATE     DEFAULT NULL,
    # Start time period of selection. If NULL (default)
    # - If is_realtime=0, then 1 hour before selected_end_time
    # - If is_realtime=1, then 5 minutes before selected_end_time
    #    selected_start_time  IN DATE     DEFAULT NULL,
    # End time period of selection. If NULL (default)
    # - If is_realtime=0, then latest AWR snapshot
    # - If is_realtime=1, then current time
    #    selected_end_time    IN DATE     DEFAULT NULL,
    # Instance ID to for which to retrieve data
    # - If -1, then current instance
    # - If number is specified, then for that instance
    # - If NULL (default), then all instances
    #    inst_id              IN NUMBER   DEFAULT NULL,
    #    dbid                 IN NUMBER   DEFAULT NULL,
    # Top N in SQL monitor list for which to retrieve SQL monitor details.
    # - If NULL (default), then retrieves top 10
    # - If 0, then retrieves no monitor list details
    #    monitor_list_detail  IN NUMBER   DEFAULT NULL,
    # Top N in Workload Top SQL list to retrieve monitor details,
    # - If NULL (default), then retrieves top 10
    # - If 0, then retrieves no monitor list details
    #    workload_sql_detail  IN NUMBER   DEFAULT NULL,
    # Maximum N latest ADDM tasks to retrieve
    # - If NULL (default), retrieves available data but no more than N
    # - If 0, then retrieves no ADDM task details
    #    addm_task_detail     IN NUMBER   DEFAULT NULL,
    #    report_reference     IN VARCHAR2 DEFAULT NULL,
    #    report_level         IN VARCHAR2 DEFAULT NULL,
    #    type                 IN VARCHAR2 DEFAULT 'ACTIVE',
    #    base_path            IN VARCHAR2 DEFAULT NULL);
    #  RETURN CLOB;
    #
    def generate_report(self):
        my_dir = "%s/%s" % (self.out_dir, self.ph_dir)
        if not os.path.isdir(my_dir):
            os.mkdir(my_dir)

        if self.rt_perfhub_report:
            name = "rt_perfhub_report"
        elif self.awr_perfhub_report:
            name = "awr_perfhub_report"
        file_name = "%s_%s.html" % (name, strftime("%Y-%m-%d_%H-%M-%S"))

        # Current instance or all instances for RAC
        if self.params['is_rac']:
            inst = "NULL"
        else:
            inst = -1
        level = self.params['out_level'].upper()

        if self.rt_perfhub_report:
            report = "is_realtime => 1,"
        elif self.awr_perfhub_report:
            report = "is_realtime => 0,"

        stmts = """set echo off pagesi 0 
set linesi 32767 trimsp on 
set long 1000000 longchunk 1000000
set heading off feedback off

spool %s
select dbms_perf.report_perfhub(%s
  selected_start_time => to_date('%s', 'yyyy-mm-dd hh24:mi'),
  selected_end_time => to_date('%s', 'yyyy-mm-dd hh24:mi'),
  inst_id => %s,
  dbid => %s,
  report_level => '%s')
from dual
/
spool off
""" % (file_name, report, self.begin_time, self.end_time, inst, self.db_id, level)

        cmd = "cd %s" % my_dir
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      cmd=cmd,
                      verbose=self.verbose)
        out = sql.run(silent=True, do_exit=False)
        file_created(my_dir + "/" + file_name)
        if self.verbose:
            for line in out:
                print(line)
