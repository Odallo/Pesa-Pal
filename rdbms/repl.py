import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.engine import (
    create_table, insert_into, select, select_join, 
    update, delete_from
)
from rdbms.parser import parse


def execute(command):
    """Execute parsed command by routing to appropriate engine function"""
    if not command:
        return None
    
    cmd_type = command["type"]
    
    if cmd_type == "create_table":
        return create_table(command["table"], command["columns"])
    
    elif cmd_type == "insert_into":
        return insert_into(command["table"], command["values"])
    
    elif cmd_type == "select":
        return select(command["table"], ordered_by_pk=True)
    
    elif cmd_type == "select_join":
        left_key, right_key = command["on"]
        
        # Handle table.column format in JOIN conditions
        if "." in left_key:
            left_key = left_key.split(".")[1]
        if "." in right_key:
            right_key = right_key.split(".")[1]
            
        return select_join(command["left"], command["right"], left_key, right_key)
    
    elif cmd_type == "update":
        return update(
            command["table"],
            command["set_column"],
            command["set_value"],
            command["where_column"],
            command["where_value"]
        )
    
    elif cmd_type == "delete_from":
        return delete_from(
            command["table"],
            command["where_column"],
            command["where_value"]
        )
    
    else:
        raise Exception(f"Unknown command type: {cmd_type}")


def format_result(result):
    """Format database results for display"""
    if result is None:
        return ""
    
    if isinstance(result, str):
        return result
    
    if isinstance(result, list):
        if not result:
            return "No results found."
        
        # Format table rows
        output = []
        for i, row in enumerate(result, 1):
            if isinstance(row, dict):
                row_str = ", ".join([f"{k}: {v}" for k, v in row.items()])
                output.append(f"{i}. {{ {row_str} }}")
            else:
                output.append(f"{i}. {row}")
        
        return "\n".join(output)
    
    return str(result)


def repl():
    """Interactive REPL for the RDBMS"""
    print("🗄️  Pesa Pal RDBMS - Interactive Shell")
    print("Type 'exit' or 'quit' to leave")
    print("Supported: CREATE TABLE, INSERT INTO, SELECT, UPDATE, DELETE FROM, JOIN")
    print()
    
    while True:
        try:
            query = input("rdbms> ").strip()
            
            if query.lower() in ("exit", "quit"):
                print("Goodbye! 👋")
                break
            
            if not query:
                continue
            
            # Parse and execute
            command = parse(query)
            result = execute(command)
            
            # Format and display result
            formatted = format_result(result)
            if formatted:
                print(formatted)
            print()
            
        except KeyboardInterrupt:
            print("\nUse 'exit' or 'quit' to leave")
        except Exception as e:
            print(f"Error: {e}")
            print()


if __name__ == "__main__":
    repl()