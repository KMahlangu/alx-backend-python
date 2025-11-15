# Python Generators - Task 0

## Objective
Create a generator that streams rows from an SQL database one by one.

## Database Schema
- **Database**: ALX_prodev
- **Table**: user_data
- **Fields**:
  - user_id (VARCHAR(36), Primary Key, Indexed)
  - name (VARCHAR(255), NOT NULL)
  - email (VARCHAR(255), NOT NULL)
  - age (DECIMAL(3,0), NOT NULL)

## Functions

### `connect_db()`
Connects to MySQL database server.

### `create_database(connection)`
Creates ALX_prodev database if it doesn't exist.

### `connect_to_prodev()`
Connects to ALX_prodev database.

### `create_table(connection)`
Creates user_data table with required fields.

### `insert_data(connection, data)`
Inserts data from CSV file into database.

### `user_data_generator(connection, batch_size=1000)`
**Generator function** that streams rows from database one by one.

## Usage
```python
import seed

# Setup database
connection = seed.connect_db()
seed.create_database(connection)
connection.close()

connection = seed.connect_to_prodev()
seed.create_table(connection)
seed.insert_data(connection, 'user_data.csv')

# Use generator
for row in seed.user_data_generator(connection):
    print(row)

connection.close()