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
    
    # Build empty index for new table
    save_index(table_name, {})

    return f"Table '{table_name}' created successfully."


def validate_type(value, expected_type):
    if expected_type == "INT":
        return isinstance(value, int)
    if expected_type == "TEXT":
        return isinstance(value, str)
    return False


from rdbms.storage import load_schema, load_rows, save_rows, load_index, save_index


def insert_into(table_name, values):
    schema = load_schema(table_name)
    rows = load_rows(table_name)

    columns = schema["columns"]
    primary_key = schema["primary_key"]

    # Handle both list and dictionary inputs
    if isinstance(values, dict):
        # Dictionary input - validate all required columns are present
        if len(values) != len(columns):
            raise Exception("Column count does not match value count")
        
        row = values.copy()
        
        # Validate types for all columns
        for col_name, col_def in columns.items():
            if col_name not in row:
                raise Exception(f"Missing value for column '{col_name}'")
            if not validate_type(row[col_name], col_def["type"]):
                raise Exception(f"Invalid type for column '{col_name}'")
                
    elif isinstance(values, list):
        # List input - original behavior
        if len(values) != len(columns):
            raise Exception("Column count does not match value count")

        row = {}
        for (col_name, col_def), value in zip(columns.items(), values):
            if not validate_type(value, col_def["type"]):
                raise Exception(f"Invalid type for column '{col_name}'")
            row[col_name] = value
    else:
        raise Exception("Values must be either a list or dictionary")

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
    
    # Update index with new row
    index = load_index(table_name)
    pk_value = str(row[primary_key])
    index[pk_value] = len(rows) - 1
    save_index(table_name, index)

    return "1 row inserted."


def build_pk_index(table_name):
    """
    Build primary key index for a table
    Maps primary key values to row positions in the rows list
    """
    schema = load_schema(table_name)
    rows = load_rows(table_name)
    
    # Find primary key column
    pk = schema["primary_key"]
    
    # Build index: pk_value -> row_position
    index = {}
    for i, row in enumerate(rows):
        index[str(row[pk])] = i
    
    save_index(table_name, index)
    return index


def inner_join(left_table, right_table, left_key, right_key):
    """
    Perform INNER JOIN between two tables
    Returns combined rows with prefixed column names
    """
    left_rows = load_rows(left_table)
    right_rows = load_rows(right_table)
    
    result = []
    
    # Nested loop join - O(n × m) baseline
    for left_row in left_rows:
        for right_row in right_rows:
            if left_row[left_key] == right_row[right_key]:
                combined = {}
                
                # Prefix columns with table names
                for k, v in left_row.items():
                    combined[f"{left_table}.{k}"] = v
                    
                for k, v in right_row.items():
                    combined[f"{right_table}.{k}"] = v
                    
                result.append(combined)
    
    return result


def inner_join_optimized(left_table, right_table, left_key, right_key):
    """
    Optimized INNER JOIN using index when right_key is a primary key
    Reduces complexity from O(n × m) to O(n + m)
    """
    left_rows = load_rows(left_table)
    right_rows = load_rows(right_table)
    right_schema = load_schema(right_table)
    
    result = []
    
    # Check if right_key is primary key (can use index)
    if right_schema.get("primary_key") == right_key:
        index = load_index(right_table)
        
        # Use index for O(1) lookups
        for left_row in left_rows:
            lookup_value = str(left_row[left_key])
            position = index.get(lookup_value)
            
            if position is not None:
                right_row = right_rows[position]
                combined = {}
                
                for k, v in left_row.items():
                    combined[f"{left_table}.{k}"] = v
                    
                for k, v in right_row.items():
                    combined[f"{right_table}.{k}"] = v
                    
                result.append(combined)
    else:
        # Fall back to nested loop join
        return inner_join(left_table, right_table, left_key, right_key)
    
    return result


def select_join(left_table, right_table, left_key, right_key, optimized=True):
    """
    High-level JOIN API that automatically chooses optimization
    Returns combined rows with prefixed column names
    """
    if optimized:
        return inner_join_optimized(left_table, right_table, left_key, right_key)
    else:
        return inner_join(left_table, right_table, left_key, right_key)


def select(table_name, ordered_by_pk=False):
    """
    Select all rows from a table
    Returns a list of dictionaries representing the rows
    If ordered_by_pk=True, returns rows ordered by primary key
    """
    schema = load_schema(table_name)
    rows = load_rows(table_name)
    
    if ordered_by_pk:
        # Sort rows by primary key using the index
        index = load_index(table_name)
        pk = schema["primary_key"]
        
        # Create a list of (pk_value, row) tuples and sort by pk_value
        sorted_rows = []
        for pk_value, pos in sorted(index.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]):
            sorted_rows.append(rows[pos])
        return sorted_rows
    
    return rows


def select_by_pk(table_name, pk_value):
    """
    Select a single row by primary key using index (O(1) lookup)
    Returns the row dictionary or None if not found
    """
    schema = load_schema(table_name)
    rows = load_rows(table_name)
    index = load_index(table_name)
    
    pos = index.get(str(pk_value))
    if pos is not None:
        return rows[pos]
    return None


def delete_from(table_name, where_column, where_value):
    """
    Delete rows from a table where column matches value
    Returns number of deleted rows
    """
    schema = load_schema(table_name)
    rows = load_rows(table_name)
    
    # Find rows to delete
    original_count = len(rows)
    rows = [row for row in rows if row[where_column] != where_value]
    deleted_count = original_count - len(rows)
    
    if deleted_count == 0:
        return "0 rows deleted."
    
    save_rows(table_name, rows)
    
    # Rebuild index after deletion (simple but inefficient)
    build_pk_index(table_name)
    
    return f"{deleted_count} row(s) deleted."


def update(table_name, set_column, set_value, where_column, where_value):
    """
    Update rows in a table where column matches value
    Returns number of updated rows
    """
    schema = load_schema(table_name)
    rows = load_rows(table_name)
    columns = schema["columns"]
    
    # Validate the set value type
    if set_column in columns:
        if not validate_type(set_value, columns[set_column]["type"]):
            raise Exception(f"Invalid type for column '{set_column}'")
    
    # Find and update matching rows
    updated_count = 0
    for row in rows:
        if row[where_column] == where_value:
            row[set_column] = set_value
            updated_count += 1
    
    if updated_count == 0:
        return "0 rows updated."
    
    save_rows(table_name, rows)
    return f"{updated_count} row(s) updated."
