from __future__ import print_function
from __future__ import absolute_import

import multiprocessing
import os
from time import strftime

from .sqlplus import SqlPlus


def generate_awr_reports(begin_id, end_id, summary, snap_ids,
                         params, verbose):
    awr = AwrReports(begin_id, end_id, snap_ids,
                     params, verbose)

    awr.generate_reports(summary)


class AwrReports:
    def __init__(self, begin_id, end_id, snap_ids, params, verbose):
        self.begin_id = begin_id
        self.end_id = end_id
        self.snap_ids = snap_ids
        self.params = params
        self.verbose = verbose

        self.db_id = params['dbid']
        self.inst_name = params['inst_name']
        self.out_dir = params['out_dir']
        self.formats = params['out_format']
        self.snap_ids = snap_ids
        self.awr_dir = "awr_reports_%s" % strftime("%Y-%m-%d_%H-%M-%S")
        self.parallel = params['parallel']

    def __str__(self):
        ret = "Class AwrReports:\n"
        ret += "- begin snap id: %s\n" % self.begin_id
        ret += "- end snap id: %s\n" % self.end_id
        ret += "- Snapshot IDs: %s\n" % self.snap_ids
        ret += "- db_id: %s\n" % self.db_id
        ret += "- inst_name: %s\n" % self.inst_name
        ret += "- out_dir: %s\n" % self.out_dir
        ret += "- awr_dir: %s\n" % self.awr_dir
        ret += "- formats: %s\n" % ','.join(self.formats)
        if self.parallel is not None:
            ret += "- parallel: %s\n" % self.parallel

        return ret

    def prepare_awr_report(self, fmt, inst, snap_id, my_dir, my_args, summary):
        if summary:
            cur_id = len(self.snap_ids[inst]) - 1
            prev_id = 0
        else:
            cur_id = snap_id
            prev_id = snap_id - 1

        if self.parallel:
            my_args.append((fmt, self.db_id, inst, self.snap_ids[inst][prev_id][0],
                            self.snap_ids[inst][cur_id][0],
                            self.snap_ids[inst][cur_id][1], my_dir,
                            self.params, self.verbose))
        else:
            generate_awr_report(fmt, self.db_id, inst,
                                self.snap_ids[inst][prev_id][0],
                                self.snap_ids[inst][cur_id][0],
                                self.snap_ids[inst][cur_id][1], my_dir,
                                self.params, self.verbose)

    def generate_reports(self, summary):
        my_dir = "%s/%s" % (self.out_dir, self.awr_dir)
        if not os.path.isdir(my_dir):
            os.mkdir(my_dir)

        my_args = []
        print("\n>>> Generating AWR reports...")
        for fmt in self.formats:
            for inst in self.snap_ids:
                if summary:
                    self.prepare_awr_report(fmt, inst, 0, my_dir, my_args, True)
                else:
                    for snap_id in range(1, len(self.snap_ids[inst])):
                        self.prepare_awr_report(fmt, inst, snap_id, my_dir, my_args, False)

                    self.prepare_awr_report(fmt, inst, 0, my_dir, my_args, True)

        if self.parallel:
            pool = multiprocessing.Pool(processes=self.parallel)
            pool.map(run_awr_parallel, my_args)
            pool.close()

        print("AWR reports were generated into the directory:")
        print(" - ", my_dir)


def generate_awr_report(report_type, db_id, inst_id,
                        start_id, end_id, end_date, awr_dir,
                        params, verbose):
    report_name = "awr_%s_%s_%s_%s.%s" % (inst_id, end_date,
                                          start_id, end_id,
                                          report_type
                                          if report_type != "text" else "txt")
    stmts = """
define inst_num=%s
define num_days=1
define dbid=%s
define begin_snap=%s
define end_snap=%s
define report_type=%s
define report_name=%s
@?/rdbms/admin/awrrpti.sql
""" % (inst_id, db_id, start_id, end_id, report_type, report_name)
    cmd = "cd %s" % awr_dir

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


def run_awr_parallel(args):
    generate_awr_report(*args)
