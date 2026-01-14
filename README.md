# MiniRDBMS (Pesapal Junior Dev Challenge 2026)

## Overview
A lightweight relational database system implemented in Python, supporting table creation, CRUD operations, primary keys, indexing, and inner joins. The system exposes a SQL-like interface via an interactive REPL.

## Features
- **SQL-like syntax** - CREATE TABLE, INSERT INTO, SELECT, UPDATE, DELETE FROM, JOIN
- **Persistent storage** using JSON files for human readability
- **Primary and unique key constraints** with integrity enforcement
- **Primary key indexing** for O(1) lookups
- **Inner joins** with index optimization
- **Interactive REPL** for ad-hoc querying

## Architecture
```
storage → engine → parser → repl
```

- **storage.py** - File persistence (schemas, rows, indexes)
- **engine.py** - Core database operations (CRUD, joins, indexing)
- **parser.py** - SQL-like query parsing
- **repl.py** - Interactive shell interface

## Quick Start

### Interactive REPL
```bash
cd "/home/odallo/Desktop Pesa Pal"
python3 rdbms/repl.py
```

### Example Session
```sql
rdbms> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE);
Table 'users' created successfully.

rdbms> INSERT INTO users VALUES (1, "Alice", "alice@email.com");
1 row inserted.

rdbms> SELECT * FROM users;
1. { id: 1, name: Alice, email: alice@email.com }

rdbms> SELECT * FROM users JOIN orders ON users.id = orders.user_id;
1. { users.id: 1, users.name: Alice, users.email: alice@email.com, orders.id: 101, orders.user_id: 1, orders.total: 500, orders.product: Laptop }
```

## Supported SQL

### CREATE TABLE
```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE);
```

### INSERT INTO
```sql
INSERT INTO users VALUES (1, "Alice", "alice@email.com");
```

### SELECT
```sql
SELECT * FROM users;
SELECT * FROM users JOIN orders ON users.id = orders.user_id;
```

### UPDATE
```sql
UPDATE users SET name="Bob" WHERE id=1;
```

### DELETE FROM
```sql
DELETE FROM users WHERE id=1;
```

## Indexing System

The system maintains a primary key index mapping PK values to row positions:

**Index Format:**
```json
{
  "1": 0,
  "2": 1,
  "3": 2
}
```

**Performance:**
- **Without index**: O(n) linear scan
- **With index**: O(1) direct lookup
- **JOIN optimization**: Uses index when join key is primary key

## File Structure
```
data/
 ├── users_schema.json     # Table definition
 ├── users_rows.json      # Table data
 └── users_pk_index.json # Primary key index
```

## Limitations
- No query optimizer
- No transactions
- Minimal SQL grammar
- Single-threaded execution
- No foreign key constraints

## Why This Design

**Focus on correctness, clarity, and explainability.**

- **JSON storage**: Human-readable persistence allows focus on relational logic
- **Simple parsing**: Token-based approach keeps the system understandable
- **Index optimization**: Demonstrates real database performance concepts
- **Clean separation**: Parser never touches files, engine never parses strings

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-------------|---------|
| SELECT by PK | O(1) | Uses index |
| SELECT all | O(n) | Linear scan |
| INSERT | O(1) | Index update |
| DELETE | O(n) | Index rebuild |
| JOIN (indexed) | O(n+m) | Index optimization |
| JOIN (nested loop) | O(n×m) | Fallback method |

## Demo Web App

A Flask demonstration is included showing real-world usage:

```bash
python3 web/app.py
```

Features user management with CRUD operations powered by the RDBMS.

## Interview Questions & Answers

**Q: Why JSON storage?**
A: JSON gave me human-readable persistence and allowed me to focus on relational logic rather than binary file formats.

**Q: How is indexing implemented?**
A: I maintain a dictionary mapping primary key values to row positions, reducing lookup from O(n) to O(1).

**Q: What happens on duplicate primary key insert?**
A: The insert is rejected before persistence, enforcing integrity constraints.

**Q: How does JOIN work?**
A: I iterate left table and use indexed key on right table to match rows efficiently.

**Q: Is this real SQL?**
A: It's a SQL-like language that covers core relational operations. The goal was clarity and correctness, not full SQL compliance.

## Technologies Used
- **Python 3** - Core implementation
- **JSON** - Data persistence
- **Flask** - Demo web application
- **HTML/CSS** - Simple frontend

---

*Built for the Pesapal Junior Developer Challenge 2026*
