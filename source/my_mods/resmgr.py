from __future__ import print_function
from __future__ import absolute_import

import re

from .sqlplus import SqlPlus
from .utils import file_created, select_arept_header, print_output


class ResourcePlanNotFound(Exception): pass


def print_resource_plan(params=None, verbose=False):
    plan = ResourcePlan(params=params, verbose=verbose)
    if verbose:
        print(plan)

    plan_name = plan.get_plan_name()
    if plan.check_maint_plan:
        plan.maint_plans = plan.get_maint_plans()
        if plan.force_plan:
            print(" => FORCE option is set for the current plan!")
        print_maint_plans(plan.maint_plans)

    if not plan.is_cdb_root:
        plans = plan.get_groups_or_subplans(plan_name)
        plan.check_subplans(plans)

    if plan.is_cdb_root:
        plan.get_cdb_plan_metadata(plan_name)
    else:
        plan.get_plan_metadata(plan_name)

    if plan.check_maint_plan:
        for name in plan.maint_plans:
            print_plan(params, verbose, name)


def print_maint_plans(plans):
    print("Found following resource plans for maintenance windows")
    for name in plans:
        print(" - %s" % name)


def print_plan(params, verbose, plan_name):
    params['resource_plan'] = plan_name

    plan = ResourcePlan(params=params, verbose=verbose, is_maint_plan=True)
    if verbose:
        print(plan)

    if plan.is_cdb_root:
        plan.get_cdb_plan_metadata(plan_name)
    else:
        plans = plan.get_groups_or_subplans(plan_name)
        plan.check_subplans(plans)
        plan.get_plan_metadata(plan_name)

class ResourcePlan:
    def __init__(self,params, verbose, is_maint_plan=False):
        self.params = params
        self.is_cdb_root = params['is_cdb'] and params['con_name'] == 'CDB$ROOT'
        self.verbose = verbose
        self.groups = set()
        self.subplans = set()
        self.pdbs = []

        self.check_maint_plan = False
        self.is_maint_plan = is_maint_plan
        self.force_plan = False
        self.maint_plans = list()
        self.history_days = 10

    def __str__(self):
        ret = "Class ResourcePlan:\n"

        if self.params:
            ret += "- Parameters:"
            for name in sorted(self.params.keys()):
                ret += "    %s: %s\n" % (name, self.params[name])

        ret += "- is_maint_plan: %s\n" % self.is_maint_plan
        ret += "- is_cdb_root: %s\n" % self.is_cdb_root

        return ret

    def get_plan_name(self):
        if self.params['resource_plan'] != 'CURRENT':
            return self.check_plan_name()

        self.check_maint_plan = True

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts="select 'PLAN#' || value || '#' from v$parameter where name = 'resource_manager_plan';",
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        for line in out:
            matchobj = re.match(r'\APLAN#\s*([:\w]+)\s*#.*\Z', line)
            if matchobj:
                my_plan = matchobj.group(1).upper()
                print("Current active resource plan: %s" % my_plan)
                if my_plan.startswith('FORCE:'):
                    self.force_plan = True
                    my_plan = my_plan[6:]
                return my_plan
        else:
            print("Error: can not find current Resource Manager plan in SQL*Plus output.")
            print_output(out)
            raise ResourcePlanNotFound("!!! No current plan found")

    def get_maint_plans(self):
        stmts = """
select 'PLANS#' || listagg(distinct b.resource_plan, ';') || '#'
from dba_scheduler_wingroup_members a, dba_scheduler_windows b
where a.window_name = b.window_name and
a.window_group_name = 'MAINTENANCE_WINDOW_GROUP'
/
"""
        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        for line in out:
            matchobj = re.match(r'\APLANS#(.*)#\Z', line)
            if matchobj:
                return matchobj.group(1).split(';')
        else:
            print("Error: can not find Maintenance Resource Manager plan in SQL*Plus output.")
            print_output(out)
            raise ResourcePlanNotFound("!!! No maintenance plan found")

    def check_plan_name(self):
        plan_name = self.params['resource_plan'].upper()
        if self.is_cdb_root:
            tab_name = "dba_cdb_rsrc_plans"
        else:
            tab_name = "dba_rsrc_plans"
        stmt = "select 'PLAN: ' || plan || ':' from %s " \
               "where plan = '%s';" % (tab_name, plan_name)

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)
        for line in out:
            matchobj = re.match(r'\APLAN:\s+(\w+)\s*:.*\Z', line)
            if matchobj:
                return matchobj.group(1)
        else:
            print("Error: can not find Resource Manager plan %s "
                  "in SQL*Plus output." % plan_name)
            print_output(out)
            raise ResourcePlanNotFound("!!! No Resource Manager plan found")

    def get_groups_or_subplans(self, plan_name):
        plans = []
        stmt = """select 'TYPE: ' || type || ' :GROUP_OR_PLAN: ' || group_or_subplan || ' :' 
from DBA_RSRC_PLAN_DIRECTIVES where plan = '%s';
""" % plan_name

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmt,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=False)

        for line in out:
            matchobj = re.match(r'\ATYPE:\s+(\w+)\s+:GROUP_OR_PLAN:\s+(\w+)\s+:.*\Z', line)
            if matchobj:
                if matchobj.group(1) == 'CONSUMER_GROUP':
                    self.groups.add(matchobj.group(2))
                elif matchobj.group(1) == 'PLAN':
                    print("Found plan:", matchobj.group(2))
                    self.subplans.add(matchobj.group(2))
                    plans.append(matchobj.group(2))

        if len(self.groups) == 0 and len(self.subplans) == 0:
            print("Error: can not find consumer groups (plan: %s) "
                  "in SQL*Plus output." % plan_name)
            print_output(out)
            raise ResourcePlanNotFound("!!! No Consumer Groups found")

        return plans

    def check_subplans(self, plans):
        if not plans:
            return []

        plan_name = plans.pop(0)
        plans = plans + self.get_groups_or_subplans(plan_name)

        return self.check_subplans(plans)

    def get_plan_metadata(self, plan_name):
        file_name = "%s/resource_plan_%s_metadata.txt" % (
            self.params['out_dir'], plan_name)
        stmts = """set pagesi 0 trimsp on long 50000 echo off
set long 500000 longchunk 1000

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s

%s

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);

select dbms_metadata.get_ddl(object_type=>'RMGR_PLAN', name=>'%s') from dual;
select dbms_metadata.get_dependent_ddl(object_type=>'RMGR_PLAN_DIRECTIVE',base_object_name=>'%s') from dual;

%s

%s

set pagesi 100 linesi 256 trimsp on echo on

col consumer_group for a30
col category for a30
col status for a10
col mandatory for a9

select consumer_group, category, status, mandatory
from dba_rsrc_consumer_groups
where consumer_group in (%s)
order by consumer_group
/

col attribute for a22
col value for a30

select consumer_group, attribute, value
from dba_rsrc_group_mappings
where consumer_group in (%s)
order by consumer_group, attribute
/

select priority, attribute 
from dba_rsrc_mapping_priority
order by priority
/

col group_or_subplan for a24
col type for a15

select group_or_subplan, type, 
parallel_degree_limit_p1 par_degree_limit, 
parallel_server_limit par_server_limit, 
utilization_limit util_limit
from dba_rsrc_plan_directives
where plan = '%s'
order by group_or_subplan 
/

col sw_group for a20
col sw_for_call for a10

select group_or_subplan, 
mgmt_p1, mgmt_p2, mgmt_p3, mgmt_p4, mgmt_p5,
switch_group sw_group, switch_for_call sw_for_call 
from dba_rsrc_plan_directives
where plan = '%s'
order by mgmt_p1 desc, mgmt_p2 desc, 
mgmt_p3 desc, mgmt_p4 desc, mgmt_p5 desc 
/

%s

%s

spool off
""" % (file_name, select_arept_header(),
       plan_name, plan_name, self.plan_ddls(),
       self.consumer_groups_ddls(),
       self.consumer_groups_in_list(),
       self.consumer_groups_in_list(),
       plan_name, plan_name,
       self.add_maintenance_window(),
       self.add_history())

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=True, do_exit=False)
        file_created(file_name, 1)
        if self.verbose:
            for line in out:
                print(line)

    def add_maintenance_window(self):
        if not self.check_maint_plan:
            return ""

        stmts = """
col window_name for a16
col resource_plan for a24
col next_start_date for a45
col duration for a15
        
prompt Resource plans for the automatic maintenance jobs.

select a.window_name, b.resource_plan, b.next_start_date, b.duration
from dba_scheduler_wingroup_members a, dba_scheduler_windows b
where a.window_name = b.window_name and
a.window_group_name = 'MAINTENANCE_WINDOW_GROUP'
order by b.next_start_date
/

"""
        return stmts

    def add_history(self):
        if self.is_maint_plan:
            return ""

        if self.params['is_rac']:
            select_clause = "a.inst_id inst, "
            from_clause = "gv$rsrc_plan_history a"
            order_by_clause = "inst, "
        else:
            select_clause = ""
            from_clause = "v$rsrc_plan_history a"
            order_by_clause = ""

        stmts = """
col plan_start for a16
col plan_end for a16
col plan_name for a24
col ebs head by_sched for a8
col window_name for a15
col cpu_managed head cpu_manag for a9
col ic for a3
col parallel_execution_managed head par_exec_manag for a13

prompt Active resource manager plans in last %d days.

select %s a.name plan_name, 
to_char(a.start_time, 'yyyy-mm-dd hh24:mi') plan_start, 
to_char(a.end_time, 'yyyy-mm-dd hh24:mi') plan_end,
a.enabled_by_scheduler ebs, a.window_name, 
a.instance_caging ic,
a.cpu_managed,
a.parallel_execution_managed
from %s 
where a.name is not null and 
a.start_time > sysdate - %d 
order by %s plan_start
/

col snap_time for a15
col cons_group for a25
col plan_name for a24
col inst for 9999
col seq for 999
col ic for a4

break on snap_time skip 1 on plan_name on seq skip 1

prompt Resource groups consumption in last 2 days.

select a.instance_number inst,
to_char(a.end_interval_time, 'dd-mon hh24:mi:ss') snap_time,
c.plan_name, b.sequence# seq, 
b.consumer_group_name cons_group,
b.requests req,
b.cpu_waits, b.yields,
round(b.cpu_wait_time/1000, 0) cpu_wait_time_s , 
round(b.consumed_cpu_time/1000, 0) cons_cpu_time_s,
b.pqs_queued, b.pqs_completed, b.pq_servers_used,
c.instance_caging ic
from dba_hist_snapshot a,
dba_hist_rsrc_consumer_group b,
dba_hist_rsrc_plan c
where a.end_interval_time > sysdate - 2
and a.dbid = b.dbid and a.instance_number = b.instance_number
and a.snap_id = b.snap_id
and b.sequence# = c.sequence#
and b.dbid = c.dbid and b.instance_number = c.instance_number
and b.snap_id = c.snap_id
order by inst, a.end_interval_time, plan_name, seq, cons_cpu_time_s desc, req desc
/

clear breaks
        
""" % (self.history_days, select_clause, from_clause,
       self.history_days, order_by_clause)

        return stmts

    def consumer_groups_ddls(self):
        ret = ""
        for name in sorted(list(self.groups)):
            ret += "select dbms_metadata.get_ddl(object_type=>'RMGR_CONSUMER_GROUP', " \
                   "name=>'%s') from dual;\n" % name
        return ret

    def consumer_groups_in_list(self):
        ret = ""
        delim = ""
        for name in self.groups:
            ret += "%s'%s'" % (delim, name)
            delim = ', '
        return ret

    def plan_ddls(self):
        ret = ""
        for name in self.subplans:
            ret += """
select dbms_metadata.get_ddl(object_type=>'RMGR_PLAN', name=>'%s') from dual;
select dbms_metadata.get_dependent_ddl(object_type=>'RMGR_PLAN_DIRECTIVE',base_object_name=>'%s') from dual;
""" % (name, name)

        return ret + "\n"

    def get_cdb_plan_metadata(self, plan_name):
        file_name = "%s/cdb_resource_plan_%s_metadata.txt" % (
            self.params['out_dir'], plan_name)
        stmts = """set pagesi 0 trimsp on long 50000 echo off
set long 500000 longchunk 1000

alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss';

spool %s

%s

set pagesi 100 linesi 256 trimsp on echo on

col pdb for a30
col status for a10
col mandatory for a9
col dir_type for a17
col profile for a15

prompt CDB Plan Directives

select pluggable_database pdb, 
profile, directive_type dir_type, 
shares,
utilization_limit util_limit,
parallel_server_limit par_server_limit,
round(memory_min/(1024*1024), 0) mem_min_mb,
round(memory_limit/(1024*1024), 0) mem_limit_mb,
status, mandatory
from dba_cdb_rsrc_plan_directives
where  plan = '%s'
order by pdb
/

%s

%s

spool off
""" % (file_name, select_arept_header(),
       plan_name,
       self.add_maintenance_window(),
       self.add_cdb_history())

        sql = SqlPlus(con=self.params['db_con'],
                      pdb=self.params['pdb'],
                      stmts=stmts,
                      out_dir=self.params['out_dir'],
                      verbose=self.verbose)
        out = sql.run(silent=True, do_exit=False)
        file_created(file_name, 1)
        if self.verbose:
            for line in out:
                print(line)

    def add_cdb_history(self):
        if self.is_maint_plan:
            return ""

        if self.params['is_rac']:
            select_clause = "a.inst_id inst, "
            from_clause = "gv$containers a, gv$rsrc_plan_history b"
            where_clause = " and a.inst_id = b.inst_id "
            order_by_clause = "inst, "
        else:
            select_clause = ""
            from_clause = "v$containers a, v$rsrc_plan_history b"
            where_clause = ""
            order_by_clause = ""

        stmts = """
prompt Active resource manager plans in last %d days.

col pdb for a15
col con_id for 999999
col plan_start for a16
col plan_end for a16
col plan_name for a24
col ebs head by_sched for a8
col window_name for a15
col cpu_managed head cpu_manag for a9
col ic for a3
col parallel_execution_managed head par_exec_manag for a13

select %s a.con_id, a.name pdb, 
b.name plan_name, 
to_char(b.start_time, 'yyyy-mm-dd hh24:mi') plan_start, 
to_char(b.end_time, 'yyyy-mm-dd hh24:mi') plan_end,
b.enabled_by_scheduler ebs, b.window_name, 
b.instance_caging ic,
b.cpu_managed,
b.parallel_execution_managed
from %s 
where b.name is not null and 
b.start_time > sysdate - %d and
a.con_id = b.con_id %s
order by %s pdb, plan_start
/

col snap_time for a15
col cons_group for a25
col inst for 9999
col seq for 999
col ic for a4

break on pdb skip 1 on snap_time skip 1 on plan_name on seq skip 1

prompt Resource groups consumption in last 2 days.

with pdb_names as 
(select distinct dbid, instance_number, con_dbid, con_id, pdb_name
from dba_hist_pdb_instance)
select d.pdb_name pdb, a.instance_number inst,
to_char(a.end_interval_time, 'dd-mon hh24:mi:ss') snap_time,
c.plan_name, b.sequence# seq,
b.consumer_group_name cons_group,
b.requests req,
b.cpu_waits, b.yields,
round(b.cpu_wait_time/1000, 0) cpu_wait_time_s , 
round(b.consumed_cpu_time/1000, 0) cons_cpu_time_s,
b.pqs_queued, b.pqs_completed, b.pq_servers_used,
c.instance_caging ic
from dba_hist_snapshot a,
dba_hist_rsrc_consumer_group b,
dba_hist_rsrc_plan c,
pdb_names d
where a.end_interval_time > sysdate - 2
and a.dbid = b.dbid and a.instance_number = b.instance_number
and a.snap_id = b.snap_id
and b.con_id = 1 and b.sequence# = c.sequence# 
and b.dbid = c.dbid and b.instance_number = c.instance_number
and b.snap_id = c.snap_id and b.con_id = c.con_id 
and c.dbid = d.dbid and c.instance_number = d.instance_number
and c.con_id = d.con_id and c.con_dbid = d.con_dbid 
order by pdb, inst, a.end_interval_time, plan_name, seq, cons_cpu_time_s desc, req desc
/

clear breaks

""" % (self.history_days, select_clause, from_clause,
       self.history_days, where_clause, order_by_clause)

        return stmts
