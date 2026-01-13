import json
import os

DATA_DIR = "data"

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        

def schema_path(table_name):
    return os.path.join(DATA_DIR, f"{table_name}_schema.json")


def row_path(table_name):
    return os.path.join(DATA_DIR, f"{table_name}_rows.json")


def save_schema(table_name, schema):
    ensure_data_dir()
    with open(schema_path(table_name), "w") as f:
        json.dump(schema, f, indent=2)


def load_schema(table_name):
    path = schema_path(table_name)
    if not os.path.exists(path):
        raise Exception(f"Table '{table_name}' does not exist")
    with open(path) as f:
        return json.load(f)


def save_rows(table_name, rows):
    ensure_data_dir()
    with open(rows_path(table_name), "w") as f:
        json.dump(rows, f, indent=2)


def load_rows(table_name):
    path = rows_path(table_name)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)