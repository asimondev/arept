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
        print_template(args.template, args.out_dir)
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
                  sql_id=args.sql_id,
                  sql_child=args.sql_child,
                  sql_format=args.awr_sql_format,
                  verbose=verbose)

    db.select_version()
    db.select_properties()

    if verbose:
        print(db)

    print("Output directory: %s" % db.out_dir)

    if db.awr_sql_ids:
        (begin_id, end_id) = db.get_awr_interval_ids()

        db.select_awr_rac_instances(begin_id, end_id)
        db.awr_sql_reports(begin_id, end_id)

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
