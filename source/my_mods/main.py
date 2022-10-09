from .parse import parse_args
from .database import Database
from .templates import print_template

AREPT_VERSION = "0.0.1"

verbose = False


def set_verbose(flag):
    global verbose
    verbose = flag


def start_arept():
    args = parse_args(AREPT_VERSION)
    set_verbose(args.verbose)

    if verbose:
        print(args)

    if args.template:
        print_template(args.template, args.out_dir, args.arept_args)
        return

    db = Database(out_dir=args.out_dir,
                  out_level=args.out_level,
                  out_format=args.out_format,
                  db_user=args.db_user,
                  db_pwd=args.db_pwd,
                  db_name=args.db_name,
                  db_internal=args.db_internal,
                  pdb=args.pdb,
                  obj_tables=args.obj_tables,
                  obj_indexes=args.obj_indexes,
                  obj_index_tables=args.obj_index_tables,
                  obj_views=args.obj_views,
                  obj_mat_views=args.obj_mat_views,
                  schema=args.schema,
                  begin_time=args.begin_time,
                  end_time=args.end_time,
                  begin_snap_id=args.begin_snap_id,
                  end_snap_id=args.end_snap_id,
                  awr_sql_ids=args.awr_sql_ids,
                  awr_sql_format=args.awr_sql_format,
                  parallel=args.parallel,
                  awr_report=args.awr_report,
                  awr_summary=args.awr_summary,
                  global_awr_report=args.global_awr_report,
                  global_awr_summary=args.global_awr_summary,
                  addm_report=args.addm_report,
                  rt_addm_report=args.rt_addm_report,
                  ash_report=args.ash_report,
                  global_ash_report=args.global_ash_report,
                  rt_perfhub_report=args.rt_perfhub_report,
                  awr_perfhub_report=args.awr_perfhub_report,
                  rt_perfhub_sql=args.rt_perfhub_sql,
                  awr_perfhub_sql=args.awr_perfhub_sql,
                  rt_perfhub_session=args.rt_perfhub_session,
                  awr_perfhub_session=args.awr_perfhub_session,
                  params=args.params,
                  sql_id=args.sql_id,
                  sql_child=args.sql_child,
                  sql_format=args.awr_sql_format,
                  wait_event_name=args.wait_event_name,
                  arept_args=args.arept_args,
                  verbose=verbose)

    db.select_version()
    db.select_properties()
    db.set_instance_number()

    if verbose:
        print(db)

    if db.wait_event_name:
        db.get_wait_event_params()
        return

    print("Output directory: %s" % db.out_dir)

    db.awr_objects()
    db.addm_reports()
    db.ash_reports()
    db.perfhub_reports()

    if db.sql_id:
        db.sql_report()

    if db.obj_tables:
        db.table_ddls()

    if db.obj_indexes:
        db.index_ddls()

    if db.obj_index_tables:
        db.index_table_ddls()

    if db.obj_views:
        db.view_ddls()

    if db.obj_mat_views:
        db.mat_view_ddls()
