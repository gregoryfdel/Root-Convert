# ROOT Convert
## Introduction
Working with ROOT files can be a pain, as they are not portable or usable outside the ROOT/C++ ecosystem with the exception of the phenomenal `uproot`. This script, `root_convert.py` takes a `.root` file and extracts the objects inside of it then converts that data into other formats.

## Pipeline
This script takes the `.root` files, converts the internal objects into `.parquet` and `.json` files; then converts the `.parquet` files into any specified format from `duckdb`, `excel`, `hdf5`, `sqlite`, `json`, or keeps it as `parquet` files. The conversions specifically are:
 - Trees -> `parquet`
 - Histograms -> `json`
 - Dynamic Objects -> `json`

JSON is used due to the flexibility of the format, while `parquet` is a fast, compressed, and portable format. All other types of objects are not implemented and PRs are welcome.

## Usage
- `pipenv run python root_convert.py <format> <input file>`
- The output is in the current directory

## JSON for Histograms
Due to the non-uniform data inside histograms, `json` was choosen; to reconstruct a histogram in a particular plotting library from the extracted data will take some work, but all of the relevant data is inside of the outtputed file.
