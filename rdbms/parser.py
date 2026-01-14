def parse_create_table(query):
    """Parse CREATE TABLE statement"""
    tokens = query.strip(";").split()
    
    if len(tokens) < 5:
        raise Exception("Invalid CREATE TABLE syntax")
    
    table_name = tokens[2]
    columns_str = " ".join(tokens[3:]).strip("()")
    column_defs = columns_str.split(",")
    
    columns = {}
    for col_def in column_defs:
        parts = col_def.strip().split()
        if len(parts) < 2:
            continue
            
        col_name = parts[0]
        col_type = parts[1]
        
        col_meta = {"type": col_type}
        
        # Handle constraints
        for part in parts[2:]:
            if part.upper() == "PRIMARY" and "KEY" in parts[parts.index(part)+1:]:
                col_meta["primary_key"] = True
            elif part.upper() == "UNIQUE":
                col_meta["unique"] = True
        
        columns[col_name] = col_meta
    
    return {
        "type": "create_table",
        "table": table_name,
        "columns": columns
    }


def parse_insert_into(query):
    """Parse INSERT INTO statement"""
    tokens = query.strip(";").split()
    
    if len(tokens) < 6:
        raise Exception("Invalid INSERT INTO syntax")
    
    table_name = tokens[2]
    
    # Extract values between parentheses
    values_str = " ".join(tokens[4:]).strip("()")
    values = []
    
    # Simple parsing - split by comma and handle quotes
    parts = values_str.split(",")
    for part in parts:
        part = part.strip()
        if part.startswith('"') and part.endswith('"'):
            values.append(part.strip('"'))
        elif part.startswith("'") and part.endswith("'"):
            values.append(part.strip("'"))
        else:
            # Try to convert to int, keep as string if fails
            try:
                values.append(int(part))
            except ValueError:
                values.append(part)
    
    return {
        "type": "insert_into",
        "table": table_name,
        "values": values
    }


def parse_select(query):
    """Parse SELECT statement"""
    tokens = query.strip(";").split()
    
    if "JOIN" in tokens:
        # Handle SELECT with JOIN
        join_idx = tokens.index("JOIN")
        on_idx = tokens.index("ON")
        
        # Get the ON condition properly
        on_condition = tokens[on_idx + 1]
        if "=" in on_condition:
            left_key, right_key = on_condition.split("=")
        else:
            # Handle case where ON condition might be split across tokens
            on_parts = []
            i = on_idx + 1
            while i < len(tokens) and "=" not in " ".join(on_parts):
                on_parts.append(tokens[i])
                i += 1
            if i < len(tokens):
                on_parts.append(tokens[i])
            on_condition = " ".join(on_parts)
            left_key, right_key = on_condition.split("=")
        
        return {
            "type": "select_join",
            "left": tokens[3],
            "right": tokens[join_idx + 1],
            "on": [left_key.strip(), right_key.strip()]
        }
    else:
        # Simple SELECT
        return {
            "type": "select",
            "table": tokens[3]
        }


def parse_update(query):
    """Parse UPDATE statement"""
    tokens = query.strip(";").split()
    
    if len(tokens) < 5:
        raise Exception("Invalid UPDATE syntax")
    
    table_name = tokens[1]
    
    # Parse SET clause
    set_idx = tokens.index("SET")
    set_clause = tokens[set_idx + 1]
    set_parts = set_clause.split("=")
    set_column = set_parts[0]
    set_value = set_parts[1].strip('"\'')
    
    # Try to convert to int
    try:
        set_value = int(set_value)
    except ValueError:
        pass
    
    # Parse WHERE clause if present
    if "WHERE" in tokens:
        where_idx = tokens.index("WHERE")
        where_clause = " ".join(tokens[where_idx + 1:])
        where_parts = where_clause.split("=")
        where_column = where_parts[0]
        where_value = where_parts[1].strip('"\'')
        
        # Try to convert to int
        try:
            where_value = int(where_value)
        except ValueError:
            pass
    else:
        raise Exception("UPDATE requires WHERE clause")
    
    return {
        "type": "update",
        "table": table_name,
        "set_column": set_column,
        "set_value": set_value,
        "where_column": where_column,
        "where_value": where_value
    }


def parse_delete_from(query):
    """Parse DELETE FROM statement"""
    tokens = query.strip(";").split()
    
    if len(tokens) < 4:
        raise Exception("Invalid DELETE FROM syntax")
    
    table_name = tokens[2]
    
    # Parse WHERE clause if present
    if "WHERE" in tokens:
        where_idx = tokens.index("WHERE")
        where_clause = " ".join(tokens[where_idx + 1:])
        where_parts = where_clause.split("=")
        where_column = where_parts[0]
        where_value = where_parts[1].strip('"\'')
        
        # Try to convert to int
        try:
            where_value = int(where_value)
        except ValueError:
            pass
    else:
        raise Exception("DELETE FROM requires WHERE clause")
    
    return {
        "type": "delete_from",
        "table": table_name,
        "where_column": where_column,
        "where_value": where_value
    }


def parse(query):
    """Main parser function - routes to specific parsers"""
    query = query.strip()
    
    if not query:
        return None
    
    if query.upper().startswith("CREATE TABLE"):
        return parse_create_table(query)
    elif query.upper().startswith("INSERT INTO"):
        return parse_insert_into(query)
    elif query.upper().startswith("SELECT"):
        return parse_select(query)
    elif query.upper().startswith("UPDATE"):
        return parse_update(query)
    elif query.upper().startswith("DELETE FROM"):
        return parse_delete_from(query)
    else:
        raise Exception(f"Unsupported query type: {query[:20]}...")