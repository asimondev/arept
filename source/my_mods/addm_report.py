from __future__ import print_function
from __future__ import absolute_import

import multiprocessing
import os
from time import strftime

from .sqlplus import SqlPlus
from .utils import file_created


def generate_addm_reports(begin_id, end_id, snap_ids,
                         params, verbose):
    addm = ADDMReports(begin_id, end_id, snap_ids,
                     params, verbose)
    if verbose:
        print(addm)

    addm.generate_reports()


def generate_rt_addm_report(params, verbose):
    addm = ADDMReports(params=params, verbose=verbose, rt_report=True)
    if verbose:
        print(addm)

    addm.generate_rt_report()


class ADDMReports:
    def __init__(self, begin_id=None,
                 end_id=None,
                 snap_ids=None,
                 params=None,
                 verbose=False,
                 rt_report=False):
        self.begin_id = begin_id
        self.end_id = end_id
        self.snap_ids = snap_ids
        self.params = params
        self.verbose = verbose
        self.rt_report = rt_report

        self.db_id = params['dbid']
        self.inst_name = params['inst_name']
        self.out_dir = params['out_dir']
        self.formats = params['out_format']
        self.snap_ids = snap_ids

        if self.rt_report:
            self.addm_dir = "addm_rt_reports"
        else:
            self.addm_dir = "addm_reports_%s" % strftime("%Y-%m-%d_%H-%M-%S")
        self.parallel = params['parallel']

    def __str__(self):
        ret = "Class ADDMReports:\n"
        if self.begin_id is not None:
            ret += "- begin snap id: %s\n" % self.begin_id
        if self.end_id is not None:
            ret += "- end snap id: %s\n" % self.end_id
        if self.snap_ids is not None:
            ret += "- Snapshot IDs: %s\n" % self.snap_ids

        ret += "- db_id: %s\n" % self.db_id
        ret += "- inst_name: %s\n" % self.inst_name
        ret += "- out_dir: %s\n" % self.out_dir
        ret += "- addm_dir: %s\n" % self.addm_dir
        ret += "- formats: %s\n" % ','.join(self.formats)
        if self.parallel is not None:
            ret += "- parallel: %s\n" % self.parallel
        ret += "- real-time ADDM report: %s\n" % self.rt_report

        return ret

    def prepare_addm_report(self, inst, snap_id, my_dir, my_args):
        cur_id = snap_id
        prev_id = snap_id - 1

        if self.parallel:
            my_args.append((self.db_id, inst, self.snap_ids[inst][prev_id][0],
                            self.snap_ids[inst][cur_id][0],
                            self.snap_ids[inst][cur_id][1], my_dir,
                            self.params, self.verbose))
        else:
            generate_addm_report(self.db_id, inst,
                                self.snap_ids[inst][prev_id][0],
                                self.snap_ids[inst][cur_id][0],
                                self.snap_ids[inst][cur_id][1], my_dir,
                                self.params, self.verbose)

    def generate_reports(self):
        my_dir = "%s/%s" % (self.out_dir, self.addm_dir)
        if not os.path.isdir(my_dir):
            os.mkdir(my_dir)

        my_args = []
        print("\n>>> Generating ADDM reports...")
        for inst in self.snap_ids:
            for snap_id in range(1, len(self.snap_ids[inst])):
                self.prepare_addm_report(inst, snap_id, my_dir, my_args)

            self.prepare_addm_report(inst, 0, my_dir, my_args)

        if self.parallel:
            pool = multiprocessing.Pool(processes=self.parallel)
            pool.map(run_addm_parallel, my_args)
            pool.close()

        print("ADDM reports were generated into the directory:")
        print(" - ", my_dir)

    def generate_rt_report(self):
        my_dir = "%s/%s" % (self.out_dir, self.addm_dir)
        if not os.path.isdir(my_dir):
            os.mkdir(my_dir)

        file_name = "addm_rt_report_%s.txt" % strftime("%Y-%m-%d_%H-%M-%S")

        stmts = """set echo off pagesi 0 
set linesi 100000 trimsp on 
set long 500000 longchunk 100000
set heading off feedback off

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

variable p_lob clob

exec :p_lob := dbms_addm.real_time_addm_report();

spool %s
print :p_lob
spool off
""" % file_name

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


def generate_addm_report(db_id, inst_id,
                        start_id, end_id, end_date, addm_dir,
                        params, verbose):
    report_name = "addm_%s_%s_%s_%s.txt" % (inst_id, end_date,
                                          start_id, end_id)
    stmts = """
define inst_num=%s
define num_days=1
define dbid=%s
define begin_snap=%s
define end_snap=%s
define report_name=%s
@?/rdbms/admin/addmrpti.sql
""" % (inst_id, db_id, start_id, end_id, report_name)
    cmd = "cd %s" % addm_dir

    sql = SqlPlus(con=params['db_con'],
                  pdb=params['pdb'],
                  stmts=stmts,
                  out_dir=params['out_dir'],
                  verbose=verbose,
                  cmd=cmd)
    out = sql.run(silent=True)
    if verbose:
        for line in out:
            print(line)


def run_addm_parallel(args):
    generate_addm_report(*args)
