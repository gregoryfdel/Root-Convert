import argparse

from pathlib import Path, PurePath

import sqlite3

import awkward as ak
import pandas as pd

from pyarrow import parquet as pq

import uproot

from utils.json_utils import SimpleJSON
from utils.logging_utils import UtilsLogger

def list_or_none(iarg):
    return list(iarg) if iarg is not None else iarg

tdir = Path(__file__).parent.resolve()

def convert_data_tree(c_tree, c_path, c_name):
    c_awk = c_tree.arrays(library="ak")
    c_table = ak.to_arrow_table(c_awk)
    pq.write_table(c_table, str(c_path / f"{c_name}.parquet"))

def convert_histogram(c_tree, c_path, c_name):
    extracted_data = {"data": c_tree.to_numpy()}
    for ci_a, c_axis in enumerate(c_tree.axes):
        axes_data = {"traits": {k: getattr(c_axis.traits, k, None) for k in dir(c_axis.traits) if not k.startswith("_")}}

        for axis_attr in ("low", "high", "edges", "labels", "centers"):
            axis_attr_data = getattr(c_axis, axis_attr, None)
            if callable(axis_attr_data):
                axis_attr_data = list_or_none(axis_attr_data())
            axes_data[axis_attr] = axis_attr_data
        extracted_data[f"data_axis_{ci_a}"] = axes_data

    attrs = ("name", "title", "kind", "values", "weighted", "errors", "variances", "counts")
    meta = {}
    for attr in attrs:
        t_attr = getattr(c_tree, attr)
        if callable(t_attr):
            extracted_data[attr] = t_attr()
        else:
            meta[attr] = t_attr
    extracted_data["meta"] = meta
    SimpleJSON(c_path / f"{c_name}.json").save_data(extracted_data)

def convert_dynamic(c_tree, c_path, c_name):
    c_obj = {k: v.tolist() if hasattr(v, "tolist") else v for k, v in c_tree.tojson().items()}
    SimpleJSON(c_path / f"{c_name}.json").save_data(c_obj)

def convert_to_parquet(root_file):
    r_name = root_file.stem
    r_path = tdir / Path(r_name)
    try:
        r_path.mkdir(parents=True, exist_ok=True)
        with uproot.open(root_file, object_cache=None) as root_fp:
            for key_name in root_fp.keys():
                UtilsLogger.info("Converting ", key_name)
                try:
                    c_tree = root_fp[key_name]
                except (uproot.deserialization.DeserializationError, NotImplementedError) as e:
                    UtilsLogger.warning("Unable to Deserialize! ", e)
                    continue
                c_name = PurePath(key_name.replace(";1", "").replace(";", "_"))

                c_parts = c_name.parts
                c_path = r_path
                for c_part in c_parts[:-1]:
                    c_path /= Path(c_part)
                c_path.mkdir(parents=True, exist_ok=True)
                
                if isinstance(c_tree, uproot.reading.ReadOnlyDirectory):
                    continue

                if hasattr(c_tree, "arrays"):
                    convert_data_tree(c_tree, c_path, c_parts[-1])
                    continue
                
                if hasattr(c_tree, "axes"):
                    convert_histogram(c_tree, c_path, c_parts[-1])
                    continue
                
                if getattr(c_tree, "__module__", None) == "uproot.dynamic":
                    convert_dynamic(c_tree, c_path, c_parts[-1])
                    continue

                UtilsLogger.warning(f"Unknown Object {c_tree} : {type(c_tree)}")
    except FileExistsError:
        UtilsLogger.info(r_name, " has already been converted to intermediate formats")

    return r_name, r_path

def make_duckdb(r_name, r_path):
    import duckdb as ddb

    UtilsLogger.warning("Building DuckDB Database")
    con = ddb.connect(database=f"{r_name}.duckdb")
    for ff in r_path.glob("*.parquet"):
        t_name = ff.name.split(".")[0]
        con.execute(
            f"CREATE TABLE {t_name} AS SELECT * FROM read_parquet('{r_name}/{t_name}.parquet')"
        )
    con.close()

def make_excel(r_name, r_path):
    UtilsLogger.info("Building Excel File")
    with pd.ExcelWriter(f"{r_name}.xlsx", mode="w") as xlWriter:
        for ff in r_path.rglob("*.parquet"):
            t_name = ff.name.split(".")[0]
            c_par_df = pd.read_parquet(ff)
            c_par_df.to_excel(xlWriter, sheet_name=t_name)

def make_hdf5(r_name, r_path):
    UtilsLogger.info("Building HDF5 File")
    hdf5_fn = f"{r_name}.hdf5"
    for ff in r_path.rglob("*.parquet"):
        t_name = ff.name.split(".")[0]
        c_par_df = pd.read_parquet(ff)
        c_par_df.to_hdf(hdf5_fn, key=t_name)

def make_sqlite(r_name, r_path):
    UtilsLogger.info("Building SQLite File")
    o_con = sqlite3.connect( f"{r_name}.sqlite")
    for ff in r_path.rglob("*.parquet"):
        t_name = ff.name.split(".")[0]
        c_par_df = pd.read_parquet(ff)
        c_par_df.to_sql(name=t_name, con=o_con)
    o_con.close()

def make_json(r_name, r_path):
    UtilsLogger.info("Building JSON File")
    rv = {}
    for ff in r_path.rglob("*.parquet"):
        t_name = ff.name.split(".")[0]
        c_par_df = pd.read_parquet(ff)
        rv[t_name] = c_par_df
    SimpleJSON(f"{r_name}.json").save_data(rv)

def parse_args():
    parser = argparse.ArgumentParser("Root Convert")
    parser.add_argument("format", help="output file format (duckdb, xlsx, hdf, sqlite, json, parquet)")
    parser.add_argument("filename", help="input root filename", type=Path)
    return parser.parse_args()

def main():
    args = parse_args()
    r_name, r_path = convert_to_parquet(args.filename)
    if args.format == "duckdb":
        make_duckdb(r_name, r_path)
    elif args.format == "xlsx":
        make_excel(r_name, r_path)
    elif args.format == "hdf":
        make_hdf5(r_name, r_path)
    elif args.format == "sqlite":
        make_sqlite(r_name, r_path)
    elif args.format == "json":
        make_json(r_name, r_path)
    elif args.format != "parquet":
        raise ValueError(f"Unknown Format {args.format}")

if __name__ == "__main__":
    main()

