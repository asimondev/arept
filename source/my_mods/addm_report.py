from __future__ import print_function
from __future__ import absolute_import

import multiprocessing
import os
from time import strftime

from .sqlplus import SqlPlus


def generate_addm_reports(begin_id, end_id, snap_ids,
                         params, verbose):
    addm = ADDMReports(begin_id, end_id, snap_ids,
                     params, verbose)

    addm.generate_reports()


class ADDMReports:
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
        self.addm_dir = "addm_reports_%s" % strftime("%Y-%m-%d_%H-%M-%S")
        self.parallel = params['parallel']

    def __str__(self):
        ret = "Class ADDMReports:\n"
        ret += "- begin snap id: %s\n" % self.begin_id
        ret += "- end snap id: %s\n" % self.end_id
        ret += "- Snapshot IDs: %s\n" % self.snap_ids
        ret += "- db_id: %s\n" % self.db_id
        ret += "- inst_name: %s\n" % self.inst_name
        ret += "- out_dir: %s\n" % self.out_dir
        ret += "- addm_dir: %s\n" % self.addm_dir
        ret += "- formats: %s\n" % ','.join(self.formats)
        if self.parallel is not None:
            ret += "- parallel: %s\n" % self.parallel

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
