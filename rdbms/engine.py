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