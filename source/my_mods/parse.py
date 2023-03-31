from __future__ import print_function
from __future__ import absolute_import

import json
import multiprocessing
import optparse
import os
import re
import shutil
import sys

from time import strftime

from .messages import  print_templates_help
from .utils import str_to_int, print_session_params


def parse_args(arept_vers):
    parser = optparse.OptionParser(version="%prog " + arept_vers)
    parser.add_option("-c", "--config", help="JSON configuration file")
    parser.add_option("-u", "--user", help="database user")
    parser.add_option("-p", "--password", help="database user password")
    parser.add_option("-d", "--database", help="TNS alias or Easy Connect")
    parser.add_option("--pdb", help="PDB name")
    parser.add_option("-o", "--output-dir", help="output directory")
    parser.add_option("--output-level",
                      help="output level {typical | all}. Default: typical")
    parser.add_option("--output-format",
                      help="output format: text, html, active-html. Default: text,html")
    parser.add_option("--obj",
                      help="object names: [table:][owner].name,..;index:[owner.]name,..;"
                           "index_only:[owner.]name,..;view:[owner.]name,...;"
                           "mview:[owner.]name...;source:[owner.]name,...")
    parser.add_option("--obj-file", help="file name with objects")
    parser.add_option("--schema", help="default schema for objects")
    parser.add_option("-b", "--begin-time",
                      help="begin time. Format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd | hh24:mi}")
    parser.add_option("-e", "--end-time",
                      help="end time. Format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd | hh24:mi | now}")
    parser.add_option("--awr-sql-id", help="SQL_IDs in AWR")
    parser.add_option("--awr-report", help="Get AWR reports",
                      action="store_true", default=False)
    parser.add_option("--awr-summary", help="Get only one AWR report for the whole interval.",
                      action="store_true", default=False)
    parser.add_option("--global-awr-report", help="Get global AWR reports",
                      action="store_true", default=False)
    parser.add_option("--global-awr-summary", help="Get global AWR reports",
                      action="store_true", default=False)
    parser.add_option("--addm-report", help="Get ADDM reports",
                      action="store_true", default=False)
    parser.add_option("--rt-addm-report", help="Get real-time ADDM report",
                      action="store_true", default=False)
    parser.add_option("--ash-report", help="Get ASH reports",
                      action="store_true", default=False)
    parser.add_option("--global-ash-report", help="Get global ASH reports",
                      action="store_true", default=False)
    parser.add_option("--rt-perfhub-report", help="Get real-time performance hub report",
                      action="store_true", default=False)
    parser.add_option("--awr-perfhub-report", help="Get AWR performance hub report",
                      action="store_true", default=False)
    parser.add_option("--rt-perfhub-sql", help="Get real-time performance hub SQL statement report")
    parser.add_option("--awr-perfhub-sql", help="Get AWR performance hub SQL statement report")
    parser.add_option("--rt-perfhub-session", help="Get real-time performance hub session report",
                      action="store_true", default=False)
    parser.add_option("--awr-perfhub-session", help="Get AWR performance hub session report",
                      action="store_true", default=False)
    parser.add_option("--instances", help="Instances list (default: all instances; 0 - current instance)")
    parser.add_option("--parallel", help="Number of parallel AWR/ADDM reports",
                      type=int)
    parser.add_option("--resource-plan", help="Get Resource Manager Plan",
                      action="store_true", default=False)
    parser.add_option("--sql-id", help="Cursor SQL_ID in shared library")
    parser.add_option("--sql-child-number", help="Cursor child nuber in shared library")
    parser.add_option("--sql-format", help="Format option in DBMS_XPLAN.DISPLAY_CURSOR like basic, typical, "
                                                "serial, all, adaptive. Default: typical")
    parser.add_option("--sid", help="Session SID number.", type=int)
    parser.add_option("--serial", help="Session serial number.", type=int)
    parser.add_option("--instance", help="Instance number (0 - current instance).", type=int)
    parser.add_option("--cleanup", help='"rm -rf *" for existing output directory',
                      action="store_true", default=False)
    parser.add_option("-v", "--verbose", help="verbose",
                      action="store_true", dest="verbose", default=False)
    parser.add_option("--begin-snap-id", help="min. snapshot ID",
                      type=int)
    parser.add_option("--end-snap-id", help="max. snapshot ID",
                      type=int)
    parser.add_option("--get-wait-event", help="Get wait event parameters description")
    parser.add_option("-t", "--template",
                      help="{process | my_sql_trace | ses_sql_trace | "
                           "meta_table | meta_role | sql_details | "
                           "awr_sql_monintor | awr_sql_monitor_list | sql_monitor | "
                           "sql_monitor_list | sql_profile | awr_sql_profile | "
                           "sql_baseline | awr_baseline | hinted_baseline | "
                           "get_awr_snap_ids | hidden_parameters | get_sql_id | "
                           "sql_shared_cursor | check_sql_id }")
    parser.add_option("--template-help", help="Show description of AREPT templates.",
                        default=False, action="store_true")

    (options, args) = parser.parse_args()
    args = [] if args is None else args

    if options.template_help:
        print_templates_help()

    prog_args = ProgArgs(
        config_file=options.config,
        db_user=options.user,
        db_pwd=options.password,
        db_name=options.database,
        pdb=options.pdb,
        begin_time=options.begin_time,
        end_time=options.end_time,
        begin_snap_id=options.begin_snap_id,
        end_snap_id=options.end_snap_id,
        out_dir=options.output_dir,
        is_verbose=options.verbose,
        output_format=options.output_format,
        output_level=options.output_level,
        parallel=options.parallel,
        cleanup=options.cleanup,
        obj=options.obj,
        obj_file=options.obj_file,
        schema=options.schema,
        awr_sql=options.awr_sql_id,
        # awr_sql_format=options.awr_sql_format,
        awr_report=options.awr_report,
        awr_summary=options.awr_summary,
        global_awr_report=options.global_awr_report,
        global_awr_summary=options.global_awr_summary,
        addm_report=options.addm_report,
        rt_addm_report=options.rt_addm_report,
        ash_report=options.ash_report,
        global_ash_report=options.global_ash_report,
        rt_perfhub_report=options.rt_perfhub_report,
        awr_perfhub_report=options.awr_perfhub_report,
        rt_perfhub_sql=options.rt_perfhub_sql,
        awr_perfhub_sql=options.awr_perfhub_sql,
        rt_perfhub_session=options.rt_perfhub_session,
        awr_perfhub_session=options.awr_perfhub_session,
        instances=options.instances,
        sql_id=options.sql_id,
        sql_child=options.sql_child_number,
        sql_format=options.sql_format,
        sid=options.sid,
        serial=options.serial,
        instance_number=options.instance,
        template=options.template,
        wait_event_name=options.get_wait_event,
        resource_plan=options.resource_plan,
        arept_args=args
    )
    prog_args.check_args()

    return prog_args


def usage():
    print("\nParameters: {-b Begin_Time -e End_Time | -n Min_Snap_ID -x Max_Snap_ID }")
    print("  -o Output_Dir ")
    print("  -f text,html,activ-html -v ")
    print("  [-p Parallel] [--version]")
    print("Time format: {yyyy-mm-dd hh24:mi | yyyy-mm-dd | hh24:mi | now}.")
    print("  Time format example: 2018-12-31 11:11")


def os_exit(out):
    for line in out:
        print(line, end='')
    sys.exit(1)


class ProgArgs:
    def __init__(self,
                 config_file=None,
                 db_user=None,
                 db_pwd=None,
                 db_name=None,
                 pdb=None,
                 begin_time=None, end_time=None,
                 begin_snap_id=None, end_snap_id=None,
                 out_dir=None, is_verbose=None,
                 output_format=None,
                 output_level=None,
                 parallel=None,
                 cleanup=None,
                 obj=None,
                 obj_file=None,
                 schema=None,
                 awr_sql=None,
                 # awr_sql_format=None,
                 awr_report=False,
                 awr_summary=False,
                 global_awr_report=False,
                 global_awr_summary=False,
                 addm_report=False,
                 rt_addm_report=False,
                 ash_report=False,
                 global_ash_report=False,
                 rt_perfhub_report=False,
                 awr_perfhub_report=False,
                 rt_perfhub_sql=None,
                 awr_perfhub_sql=None,
                 rt_perfhub_session=False,
                 awr_perfhub_session=False,
                 instances=None,
                 sql_id=None,
                 sql_child=None,
                 sql_format=None,
                 sid=None,
                 serial=None,
                 instance_number=None,
                 template=None,
                 wait_event_name=None,
                 resource_plan=None,
                 arept_args=None
                 ):

        self.config_file = config_file
        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_name = db_name
        self.pdb = pdb
        self.db_internal = False

        self.begin_time = begin_time
        self.end_time = end_time
        self.begin_snap_id = begin_snap_id
        self.end_snap_id = end_snap_id
        self.interval_ids = None

        self.out_dir = out_dir
        self.output_format = output_format
        self.output_level = output_level

        self.out_level = None
        self.out_format = None

        # self.sql_id = sql_id
        # self.sql_ids = []

        self.verbose = is_verbose
        # set_verbose(False if is_verbose is None else is_verbose)
        self.parallel = parallel
        self.instances = instances
        self.inst_ids = []
        self.cleanup = cleanup if cleanup is not None else False

        self.obj = obj
        self.obj_tables = []
        self.obj_indexes = []
        self.obj_index_tables = []
        self.obj_views = []
        self.obj_mat_views = []
        self.obj_sources = []
        self.obj_file = obj_file
        self.schema = schema

        self.awr_sql = awr_sql
        self.awr_sql_ids = []
        # self.awr_sql_format = awr_sql_format
        self.awr_report = awr_report
        self.global_awr_report = global_awr_report
        self.global_awr_summary = global_awr_summary
        self.awr_summary = awr_summary
        self.addm_report = addm_report
        self.rt_addm_report = rt_addm_report
        self.ash_report = ash_report
        self.global_ash_report = global_ash_report
        self.rt_perfhub_report = rt_perfhub_report
        self.awr_perfhub_report = awr_perfhub_report
        self.rt_perfhub_sql = rt_perfhub_sql
        self.awr_perfhub_sql = awr_perfhub_sql
        self.rt_perfhub_session = rt_perfhub_session
        self.awr_perfhub_session = awr_perfhub_session

        self.sql_id = sql_id
        self.sql_child = sql_child
        self.sql_format = sql_format

        self.params = {
            'sid': sid, 'serial': serial, 'instance_number': instance_number
        }

        self.template = template
        self.arept_args = arept_args

        self.wait_event_name = wait_event_name
        self.resource_plan = resource_plan

        self.arept_flags={}

    def __str__(self):
        ret = "Class ProgArgs:\n"
        if self.config_file:
            ret += "- JSON configuration file: %s\n" % self.config_file
        if self.db_name:
            ret += "- database user: %s\n" % self.db_user
        if self.db_pwd:
            ret += "- database user password: %s\n" % self.db_pwd
        if self.db_name:
            ret += "- database name: %s\n" % self.db_name
        if self.pdb:
            ret += "- PDB: %s\n" % self.pdb

        if self.begin_time:
            ret += "- begin_time: %s\n" % self.begin_time
        if self.end_time:
            ret += "- end_time: %s\n" % self.end_time
        if self.begin_snap_id:
            ret += "- begin_snap_id: %s\n" % self.begin_snap_id
        if self.end_snap_id:
            ret += "- end_snap_id: %s\n" % self.end_snap_id
        if self.awr_sql:
            ret += "- AWR SQL_ID(s): %s\n" % self.awr_sql
        # if self.awr_sql_format:
        #     ret += "- AWR SQL format: %s\n" % self.awr_sql_format
        ret += "- AWR report: %s\n" % self.awr_report
        ret += "- AWR summary reports: %s\n" % self.awr_summary
        ret += "- global AWR reports: %s\n" % self.global_awr_report
        ret += "- global AWR summary report: %s\n" % self.global_awr_summary
        ret += "- ADDM report: %s\n" % self.addm_report
        ret += "- real-time ADDM report: %s\n" % self.rt_addm_report
        ret += "- ASH report: %s\n" % self.ash_report
        ret += "- global ASH report: %s\n" % self.global_ash_report
        ret += "- real-time performance hub report: %s\n" % self.rt_perfhub_report
        ret += "- AWR performance hub report: %s\n" % self.awr_perfhub_report
        if self.rt_perfhub_sql:
            ret += "- real-time performance hub SQL report: %s\n" % self.rt_perfhub_sql
        if self.awr_perfhub_sql:
            ret += "- AWR performance hub SQL report: %s\n" % self.awr_perfhub_sql
        ret += "- real-time performance hub session report: %s\n" % self.rt_perfhub_session
        ret += "- AWR performance hub session report: %s\n" % self.awr_perfhub_session

        if self.sql_id:
            ret += " - SQL_ID: %s\n" % self.sql_id
        if self.sql_child:
            ret += " - SQL child number: %d\n" % self.sql_child
        if self.sql_format:
            ret += " - Format option: %s\n" % self.sql_format

        if self.out_dir:
            ret += "- out_dir: %s\n" % self.out_dir
        if self.verbose:
            ret += "- verbose: %s\n" % self.verbose
        if self.out_format:
            ret += "- output format: %s\n" % ','.join(self.out_format)
        if self.out_level:
            ret += "- output level: %s\n" % self.out_level
        if self.parallel:
            ret += "- parallel: %s\n" % self.parallel
        if self.obj:
            ret += "- obj: %s\n" % self.obj
        if self.obj_file:
            ret += "- obj_file: %s\n" % self.obj_file
        if self.schema:
            ret += "- schema: %s\n" % self.schema

        ret += print_session_params(self.params)

        if self.wait_event_name:
            ret += "- wait event parameters: %s\n" % self.wait_event_name
        ret += " - resource manager plan: %s\n" % self.resource_plan

        if self.template:
            ret += "- template: %s\n" % self.template
        if self.arept_args:
            ret += "- args: %s\n" % self.arept_args

        return ret

    def check_args(self):
        if self.config_file is not None:
            try:
                self.read_json()
            except Exception as e:
                print("Error: can't read JSON configuration file '%s'" % self.config_file)
                print(e.args)
                return False

        self.check_db_account()
        self.check_output()
        self.check_objects()
        self.check_schema()
        self.check_awr()
        self.check_addm()
        self.check_ash()
        self.check_perfhub()
        self.check_instances()
        self.check_sql()
        self.check_template()
        self.check_resource_plan()

        if self.parallel:
            cpus = multiprocessing.cpu_count()
            if self.parallel > cpus:
                print("Error: parallel parameter (%s) exceeded the "
                      "number of available CPUs(%s)." % (self.parallel, cpus))
                sys.exit(1)

    def check_awr_reports(self):
        return (self.awr_report or self.awr_summary or
                self.global_awr_report or self.global_awr_summary)

    def check_addm_reports(self):
        return self.addm_report or self.rt_addm_report

    def check_ash_reports(self):
        return self.ash_report or self.global_ash_report

    def check_perfhub_reports(self):
        return (self.rt_perfhub_report or self.awr_perfhub_report or
                self.rt_perfhub_sql is not None or
                self.awr_perfhub_sql is not None or
                self.rt_perfhub_session or
                self.awr_perfhub_session)

    def check_awr(self):
        if self.check_awr_reports():
            if self.check_awr_interval():
                return

        elif self.awr_sql:
            # self.awr_sql_format = self.check_awr_sql_format()
            self.awr_sql_ids = self.check_awr_sql_ids()
            if len(self.awr_sql_ids):
                self.check_awr_interval()

    def check_ash(self):
        if not self.check_ash_reports():
            return

        if ((self.check_awr_reports() or self.check_addm_reports()) and
                self.check_ash_reports()):
            print("Error: only one of AWR/ADDM/ASH report option can be specified.")
            sys.exit(1)

        if not self.check_awr_time_interval():
            print("Error: wrong ASH interval.")
            sys.exit(1)

    def check_perfhub(self):
        if not self.check_perfhub_reports():
            return

        if ((self.check_awr_reports() or self.check_addm_reports() or
                self.check_ash_reports()) and self.check_perfhub_reports()):
            print("Error: only one of AWR/ADDM/ASH/Performance Hub report option can be specified.")
            sys.exit(1)

        if self.rt_perfhub_report and self.awr_perfhub_report:
            print("Error: either real-time or AWR performance hub report can be specified.")
            sys.exit(1)

        if self.rt_perfhub_sql and self.awr_perfhub_sql:
            print("Error: either real-time or AWR performance hub SQL report can be specified.")
            sys.exit(1)

        if self.rt_perfhub_session and self.awr_perfhub_session:
            print("Error: either real-time or AWR performance hub session report can be specified.")
            sys.exit(1)

        if self.rt_perfhub_session or self.awr_perfhub_session:
            if not self.params['sid'] or not self.params['serial']:
                print("Error: session SID and serial number are missing for performance "
                      "hub session report")
                sys.exit(1)

        if not self.check_awr_time_interval():
            print("Error: wrong performance hub interval.")
            sys.exit(1)

    def check_instances(self):
        if (not self.rt_perfhub_session and not self.awr_perfhub_session and
                    self.params['instance_number'] is not None):
            print("Error: instance option is only used for performance hub "
                  "session reports. Did you wanted to used instances options instead?")
            sys.exit(1)

        if not self.instances:
            return

        for inst_id in self.instances.split(','):
            self.inst_ids.append(inst_id)

    def check_addm(self):
        if not self.addm_report and not self.rt_addm_report:
            return

        if self.addm_report and self.rt_addm_report:
            print("Error: either real-time or regular ADDM report can be specified.")
            sys.exit(1)

        if ((self.check_awr_reports() or self.global_ash_report or
             self.ash_report) and (self.addm_report or self.rt_addm_report)):
            print("Error: only one of AWR/ADDM/ASH report option can be specified.")
            sys.exit(1)

        if self.rt_addm_report:
            return

        if not self.check_awr_interval():
            print("Error: wrong ADDM interval.")
            sys.exit(1)

    def check_sql(self):
        self.sql_child = str_to_int(self.sql_child,
                                    "Error: SQL child number %s must be a number.")

    def check_resource_plan(self):
        if self.resource_plan:
            if self.arept_args:
                self.arept_flags['resource_plan'] = self.arept_args[0]
            else:
                self.arept_flags['resource_plan'] = 'CURRENT'
        else:
            self.arept_flags['resource_plan'] = ''

    def check_awr_sql_ids(self):
        if self.awr_sql:
            lst = self.awr_sql.split(",")
            if len(lst):
                return lst

        return self.awr_sql_ids

    # def check_awr_sql_format(self):
    #     ret = ["text", "html"]
    #     if self.awr_sql_format is None:
    #         return ret
    #
    #     lst = [x.lower() for x in self.awr_sql_format.split(',')]
    #     res = set(lst)
    #     valid = True
    #     if res:
    #         formats = set(["html", "text"])
    #         if res - formats:
    #             valid = False
    #     else:
    #         valid = False
    #
    #     if valid:
    #         return list(res)
    #     else:
    #         print("Error: wrong AWR SQL format parameter %s." % self.awr_sql_format)
    #         sys.exit(1)

    # Return True, if Ok.
    def check_awr_interval(self):
        time_ivl = self.check_awr_time_interval()
        ids_ivl = self.check_awr_ids_interval()

        if time_ivl and ids_ivl:
            print("Error: found both time and IDs for AWR interval.")
            sys.exit(1)

        if not time_ivl and not ids_ivl:
            print("Error: AWR interval is missing.")
            sys.exit(1)

        return True

    # Return True, if time interval is specified.
    def check_awr_time_interval(self):
        if self.begin_time is None and self.end_time is None:
            return False

        if self.begin_time is None or self.end_time is None:
            return False

        ts = TimeStamp(self.begin_time, False, "begin time")
        self.begin_time = ts.check()

        ts = TimeStamp(self.end_time, True, "end time")
        self.end_time = ts.check()

        if self.begin_time is None or self.end_time is None:
            print("Error: wrong time interval: %s and %s." %
                  (self.begin_time, self.end_time))
            sys.exit(1)

        return True

    def check_awr_ids_interval(self):
        if self.begin_snap_id is None and self.end_snap_id is None:
            return False

        if self.begin_snap_id is None or self.end_snap_id is None:
            return False

        if self.begin_snap_id >= self.end_snap_id:
            print("Error: wrong AWR snapshot IDs: %s and %s" %
                  (self.begin_snap_id, self.end_snap_id))
            sys.exit(1)

        return True

    def check_db_account(self):
        if self.db_user is None:
            self.db_user = os.getenv("AREPT_DB_USER")
        if self.db_pwd is None:
            self.db_pwd = os.getenv("AREPT_DB_PASSWORD")
        if self.db_name is None:
            self.db_name = os.getenv("AREPT_DB_NAME")

        self.db_internal = (self.db_name is None and
                            self.db_pwd is None and self.db_name is None)
        if self.db_internal:
            return

        if self.db_pwd is not None and self.db_user is None:
            print("Error: database user is missing")
            sys.exit(1)

        if (self.db_name is not None and (
                self.db_user is None or self.db_pwd is None)):
            print("Error: database user is missing.")
            sys.exit(1)

    def check_output(self):
        self.check_output_dir()
        self.out_level = self.check_output_level()
        self.out_format = self.check_output_format()

    def check_output_dir(self):
        if self.out_dir is None:
            self.out_dir = os.path.join(os.getcwd(), "arept_output")

        if os.path.isdir(self.out_dir):
            if self.cleanup:
                try:
                    for name in os.listdir(self.out_dir):
                        a = os.path.join(self.out_dir, name)
                        if os.path.isdir(a):
                            shutil.rmtree(a)
                        elif os.path.isfile(a):
                            os.remove(a)
                except Exception as e:
                    print("Error: can not cleanup output directory", self.out_dir, ":", e)
                    sys.exit(1)

        else:
            os.mkdir(self.out_dir)

    def check_output_level(self):
        if self.output_level is None:
            return "typical"

        level = self.output_level.lower()
        if level != "typical" and level != "all":
            print("Error: wrong output level parameter %s." % self.output_level)
            sys.exit(1)

        return self.output_level

    def check_output_format(self):
        ret = ["text", "html"]
        if self.output_format is None:
            return ret

        lst = [x.lower() for x in self.output_format.split(',')]
        res = set(lst)
        valid = True
        if res:
            formats = set(["html", "active-html", "text"])
            if res - formats:
                valid = False
        else:
            valid = False

        if valid:
            return list(res)
        else:
            print("Error: wrong output format parameter %s." % self.output_format)
            sys.exit(1)

    def check_objects(self):
        if self.obj is not None and self.obj_file is not None:
            print("Error: either object or objects file can be specified.")
            sys.exit(1)

        if self.obj is not None:
            self.check_obj()

    def check_obj(self):
        lst = self.obj.split(";")
        for a in lst:
            pos = a.find(":")
            if pos == -1 and len(self.obj_tables) == 0:  # default objects are tables
                self.obj_tables = a.split(",")
                if len(self.obj_tables) == 0:
                    print("Error: wrong table names in obj parameter: %s" % self.obj)
                    sys.exit(1)
            else:
                obj_type = a[:pos]
                if obj_type.lower() == "table" and len(self.obj_tables) == 0:
                    self.obj_tables = a[pos + 1:].split(",")
                    if len(self.obj_tables) == 0:
                        print("Error: wrong table names in obj parameter: %s" % self.obj)
                        sys.exit(1)
                elif obj_type.lower() == "index_only":
                    self.obj_indexes = a[pos + 1:].split(",")
                    if len(self.obj_indexes) == 0:
                        print("Error: wrong index_only names in obj parameter: %s" % self.obj)
                        sys.exit(1)
                elif obj_type.lower() == "index":
                    self.obj_index_tables = a[pos + 1:].split(",")
                    if len(self.obj_index_tables) == 0:
                        print("Error: wrong index names in obj parameter: %s" % self.obj)
                        sys.exit(1)
                elif obj_type.lower() == "view":
                    self.obj_views = a[pos + 1:].split(",")
                    if len(self.obj_views) == 0:
                        print("Error: wrong view names in obj parameter: %s" % self.obj)
                        sys.exit(1)
                elif obj_type.lower() == "mview":
                    self.obj_mat_views = a[pos + 1:].split(",")
                    if len(self.obj_mat_views) == 0:
                        print("Error: wrong mat. view names in obj parameter: %s" % self.obj)
                        sys.exit(1)
                elif obj_type.lower() == "source":
                    self.obj_sources = a[pos + 1:].split(",")
                    if len(self.obj_sources) == 0:
                        print("Error: wrong source names in obj parameter: %s" % self.obj)
                        sys.exit(1)

    def check_schema(self):
        if (self.schema is not None and (self.obj is None and
                                         self.obj_file is None)):
            print("Error: schema parameter must be used with object parameters.")
            sys.exit(1)

    def check_template(self):
        if self.template is None:
            return

        templates = ['process', 'my_sql_trace', 'ses_sql_trace',
                     'meta_table', "meta_role", "sql_details",
                     'awr_sql_monitor', 'awr_sql_monitor_list',
                     'sql_monitor', 'sql_monitor_list', 'sql_profile', 'awr_sql_profile',
                     'get_awr_snap_ids', 'hidden_parameters', 'sql_baseline', 'awr_baseline',
                     'hinted_baseline', 'get_sql_id', "sql_shared_cursor",
                     "check_sql_id"]
        if self.template not in templates:
            print('Error: unknown template value "%s".' % self.template)
            sys.exit(1)

    def read_json(self):
        data = json.load(open(self.config_file))

        if (self.db_user is None and 'db-user' in data and
                data['db-user'] is not None):
            self.db_user = data['db-user']

        if (self.db_pwd is None and 'db-password' in data and
                data['db-password'] is not None):
            self.db_pwd = data['db-password']

        if (self.db_name is None and 'db-name' in data and
                data['db-name'] is not None):
            self.db_name = data['db-name']

        if (self.parallel is None and 'parallel' in data and
                data['parallel'] is not None):
            self.parallel = data['parallel']

        if (self.out_dir is None and 'output-dir' in data and
                data['output-dir'] is not None):
            self.out_dir = data['output-dir']

        if (self.output_format is None and 'output-format' in data and
                data['output-format'] is not None):
            self.output_format = data['output-format']

        if (self.output_level is None and 'output-level' in data and
                data['output-level'] is not None):
            self.output_level = data['output-level']

        if (self.schema is None and 'schema' in data and
                data['schema'] is not None):
            self.schema = data['schema']

        if (self.obj is None and 'obj' in data and
                data['obj'] is not None):
            self.obj = data['obj']

        if (self.obj_file is None and 'obj-file' in data and
                data['obj-file'] is not None):
            self.obj_file = data['obj_file']

        if (self.sql_id is None and 'sql_id' in data and
                data['sql_id'] is not None):
            self.sql_id = data['sql_id']
        if (self.sql_child is None and 'sql-child' in data and
                data['sql-child'] is not None):
            self.sql_child = data['sql-child']
        if (self.sql_format is None and 'sql-format' in data and
                data['sql-format'] is not None):
            self.sql_format = data['sql-format']

        if (self.wait_event_name is None and 'get-wait-event' in data and
                data['get-wait-event'] is not None):
            self.wait_event_name = data['get-wait-event']

        if (self.template is None and 'template' in data and
                data['template'] is not None):
            self.template = data['template']


class TimeStamp:
    def __init__(self, date_str=None, now_allowed=False, name=None):
        self.date_str = date_str
        self.now_allowed = now_allowed
        self.name = name

    def __str__(self):
        ret = "Class TimeStamp:\n"
        if self.date_str:
            ret += "- date_str: %s\n" % self.date_str
        ret += "- now_allowed: %s\n" % self.now_allowed
        if self.name:
            ret += "- name: %s\n" % self.name

        return ret

    def check_timestamp_format(self):
        pattern = re.compile(r'\A20[12]\d-[01]\d-[0-3]\d [012]\d:[0-5]\d\Z')
        return pattern.match(self.date_str) is not None

    def check_date_format(self):
        pattern = re.compile(r'\A20[12]\d-[01]\d-[0-3]\d\Z')
        return pattern.match(self.date_str) is not None

    def check_time_format(self):
        pattern = re.compile(r'\A[012]\d:[0-5]\d\Z')
        return pattern.match(self.date_str) is not None

    def check(self):
        if self.date_str is None:
            print("Error: missing mandatory parameters %s." % self.name)
            return None

        elif self.check_date_format():
            return self.date_str + " 00:00"

        elif self.check_timestamp_format():
            return self.date_str

        elif self.check_time_format():
            return strftime("%Y-%m-%d") + " " + self.date_str

        elif self.now_allowed and self.date_str.lower() == "now":
            return strftime("%Y-%m-%d %H:%M")

        else:
            print("Error: wrong format for %s '%s'" %
                  (self.name, self.date_str))

        return None
