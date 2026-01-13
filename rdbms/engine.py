from tarfile import SUPPORTED_TYPES
from rdbms.storage import save_schema, save_rows, load_schema

SUPPORTED_TYPES = {"INT", "TEXT"}

def create_table(table_name, columns):
    """
    columns format:
    {
       "id": {"type": "INT", "primary_key": True},
        "email": {"type": "TEXT", "unique": True}
    }
    """

# 1. Ensuring a table does not already exist before creating one
    try:
        load_schema(table_name)
        raise Exception(f"Table '{table_name}' already exists")
    except Exception:
        pass  # Table does not exist, continue

    # 2. Validating the schema
    primary_keys = []

    for col_name, col_def in columns.items():
        col_type = col_def.get("type")

        if col_type not in SUPPORTED_TYPES:
            raise Exception(f"Unsupported type '{col_type}' for column '{col_name}'")

        if col_def.get("primary_key"):
            primary_keys.append(col_name)

    if len(primary_keys) == 0:
        raise Exception("No primary key defined")

    if len(primary_keys) > 1:
        raise Exception("Multiple primary keys are not allowed")

    # 3. Building the object
    schema = {
        "table": table_name,
        "columns": columns,
        "primary_key": primary_keys[0]
    }

    # 4. saving the schema and the created table
    save_schema(table_name, schema)
    save_rows(table_name, [])

    return f"Table '{table_name}' created successfully."


def validate_type(value, expected_type):
    if expected_type == "INT":
        return isinstance(value, int)
    if expected_type == "TEXT":
        return isinstance(value, str)
    return False


from rdbms.storage import load_schema, load_rows, save_rows


def insert_into(table_name, values):
    schema = load_schema(table_name)
    rows = load_rows(table_name)

    columns = schema["columns"]
    primary_key = schema["primary_key"]

    if len(values) != len(columns):
        raise Exception("Column count does not match value count")

    row = {}
    for (col_name, col_def), value in zip(columns.items(), values):
        if not validate_type(value, col_def["type"]):
            raise Exception(f"Invalid type for column '{col_name}'")

        row[col_name] = value

    # Primary key uniqueness
    for existing in rows:
        if existing[primary_key] == row[primary_key]:
            raise Exception("Primary key constraint violated")

    # Unique constraints
    for col_name, col_def in columns.items():
        if col_def.get("unique"):
            for existing in rows:
                if existing[col_name] == row[col_name]:
                    raise Exception(f"Unique constraint violated on '{col_name}'")

    rows.append(row)
    save_rows(table_name, rows)

    return "1 row inserted."
