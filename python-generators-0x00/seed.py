

"""
MySQL Database Seeding Script for ALX_prodev
Python Generators Task - Streaming data from MySQL
"""

import mysql.connector
import csv
import uuid
from mysql.connector import Error
import getpass


def connect_db():
    """
    Connects to the MySQL database server
    
    Returns:
        connection: MySQL connection object or None if failed
    """
    try:
        # Try common passwords first
        passwords_to_try = ['', 'password', 'root', '#Thando@2019']
        
        for password in passwords_to_try:
            try:
                print(f" Trying password: '{password}'...")
                connection = mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password=password
                )
                if connection.is_connected():
                    print(f" Connected to MySQL server successfully!")
                    print(f" Using password: '{password}'")
                    return connection
            except Error:
                continue
        
        # If common passwords don't work, ask user
        print("❌ Could not connect with common passwords.")
        user_password = getpass.getpass("🔐 Enter your MySQL root password: ")
        
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password=user_password
        )
        if connection.is_connected():
            print("✅ Connected to MySQL server successfully!")
            return connection
            
    except Error as e:
        print(f"❌ Error while connecting to MySQL: {e}")
        return None


def create_database(connection):
    """
    Creates the database ALX_prodev if it does not exist
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("✅ Database ALX_prodev created or already exists")
        cursor.close()
    except Error as e:
        print(f"❌ Error creating database: {e}")


def connect_to_prodev():
    """
    Connects to the ALX_prodev database in MySQL
    
    Returns:
        connection: MySQL connection object or None if failed
    """
    try:
        # Try to get connection details from the main connection
        connection = connect_db()
        if not connection:
            return None
            
        # First create the database if it doesn't exist
        create_database(connection)
        connection.close()
        
        # Now connect specifically to ALX_prodev
        passwords_to_try = ['', 'password', 'root', '123456']
        
        for password in passwords_to_try:
            try:
                connection = mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password=password,
                    database='ALX_prodev'
                )
                if connection.is_connected():
                    print("✅ Connected to ALX_prodev database successfully!")
                    return connection
            except Error:
                continue
        
        # If common passwords don't work, ask user
        user_password = getpass.getpass("🔐 Enter your MySQL root password for ALX_prodev: ")
        
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password=user_password,
            database='ALX_prodev'
        )
        if connection.is_connected():
            print("✅ Connected to ALX_prodev database successfully!")
            return connection
            
    except Error as e:
        print(f"❌ Error while connecting to ALX_prodev database: {e}")
        return None


def create_table(connection):
    """
    Creates a table user_data if it does not exist with the required fields
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age INT NOT NULL,
            INDEX idx_user_id (user_id)
        )
        """
        
        cursor.execute(create_table_query)
        print("✅ Table user_data created successfully")
        cursor.close()
    except Error as e:
        print(f"❌ Error creating table: {e}")


def create_sample_csv():
    """
    Creates a sample CSV file with user data for testing
    """
    sample_data = [
        ["user_id", "name", "email", "age"],
        ["0034f4e1-2d3b-4c5a-654c-3b2a1e4f5d6c", "Kutlwano Mahlangu", "mosaprojects1@gmail.com", "30"]
    ]
    
    try:
        with open('user_data.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(sample_data)
        print("✅ Sample user_data.csv created successfully with 15 users")
        return True
    except Exception as e:
        print(f"❌ Error creating sample CSV: {e}")
        return False


def insert_data(connection, csv_file_path):
    """
    Inserts data in the database if it does not exist
    
    Args:
        connection: MySQL connection object
        csv_file_path: Path to the CSV file containing user data
    """
    try:
        cursor = connection.cursor()
        
        # Check if table is empty
        cursor.execute("SELECT COUNT(*) FROM user_data")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print("ℹ️ Data already exists in the table. Skipping insertion.")
            cursor.close()
            return
        
        # Create sample CSV if it doesn't exist
        import os
        if not os.path.exists(csv_file_path):
            print(f"📝 CSV file {csv_file_path} not found. Creating sample data...")
            if not create_sample_csv():
                return
        
        # Read and insert data from CSV
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header row
            
            insert_query = """
            INSERT INTO user_data (user_id, name, email, age)
            VALUES (%s, %s, %s, %s)
            """
            
            batch_data = []
            for row in csv_reader:
                if len(row) >= 4:
                    user_id = row[0] if row[0] else str(uuid.uuid4())
                    name = row[1]
                    email = row[2]
                    age = int(row[3]) if row[3].isdigit() else 0
                    
                    batch_data.append((user_id, name, email, age))
            
            # Insert all data
            cursor.executemany(insert_query, batch_data)
            connection.commit()
            print(f"✅ Inserted {len(batch_data)} records successfully into MySQL")
        
        cursor.close()
    except Error as e:
        print(f"❌ Error inserting data: {e}")
        connection.rollback()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def user_data_generator(connection, batch_size=5):
    """
    🎪 GENERATOR FUNCTION - The Magic Part!
    Streams rows from user_data table one by one from MySQL
    
    Args:
        connection: MySQL connection object
        batch_size: Number of rows to fetch at a time
    
    Yields:
        tuple: A row from the user_data table
    """
    try:
        cursor = connection.cursor()
        
        # Get total count for progress tracking
        cursor.execute("SELECT COUNT(*) FROM user_data")
        total_rows = cursor.fetchone()[0]
        
        print(f"🎯 MySQL Generator: Streaming {total_rows} rows (batch size: {batch_size})")
        print("💡 Remember: Generator gives data ONE ROW AT A TIME from MySQL!")
        
        offset = 0
        processed_count = 0
        
        while True:
            # Get a small batch from MySQL database
            query = f"SELECT user_id, name, email, age FROM user_data LIMIT {batch_size} OFFSET {offset}"
            cursor.execute(query)
            
            rows = cursor.fetchall()
            if not rows:  # No more data
                break
            
            # 🎪 MAGIC: yield each row one by one
            for row in rows:
                processed_count += 1
                user_id, name, email, age = row
                print(f"📦 MySQL Generator YIELDING row {processed_count}: {name} (Age: {age})")
                yield row  # ⏸️ This PAUSES the function and returns one row
            
            offset += batch_size
            print(f"📊 Processed batch up to row {processed_count}")
        
        cursor.close()
        print(f"✅ MySQL Generator finished! Processed {processed_count} rows total")
        
    except Error as e:
        print(f"❌ Error in MySQL generator: {e}")
        yield None


def demonstrate_generator_magic():
    """
    🎩 Demonstrates the power of generators with real MySQL data
    """
    print("\n" + "="*70)
    print("🎩 MYSQL GENERATOR MAGIC DEMONSTRATION")
    print("="*70)
    
    # Connect to MySQL database
    connection = connect_to_prodev()
    if not connection:
        print("❌ Cannot demonstrate generator without MySQL connection")
        return
    
    print("\n🔮 Creating MySQL generator...")
    user_gen = user_data_generator(connection, batch_size=3)
    
    print("🚀 Generator created! MySQL query hasn't run yet...")
    print("💤 Generator is SLEEPING until we ask for data")
    
    # Get first user - this triggers the first MySQL query
    print("\n📞 Calling next(user_gen) for the FIRST time:")
    print("   ⚡ This executes the first MySQL query with LIMIT 3")
    first_user = next(user_gen)
    user_id, name, email, age = first_user
    print(f"   👤 First user from MySQL: {name} (Age: {age})")
    
    # Get second user
    print("\n📞 Calling next(user_gen) for the SECOND time:")
    print("   💫 No new MySQL query - using cached batch")
    second_user = next(user_gen)
    user_id, name, email, age = second_user
    print(f"   👤 Second user from MySQL: {name} (Age: {age})")
    
    # Get third user
    print("\n📞 Calling next(user_gen) for the THIRD time:")
    print("   💫 Still using cached batch from first query")
    third_user = next(user_gen)
    user_id, name, email, age = third_user
    print(f"   👤 Third user from MySQL: {name} (Age: {age})")
    
    # Get fourth user - this triggers a new MySQL query
    print("\n📞 Calling next(user_gen) for the FOURTH time:")
    print("   ⚡ This executes a new MySQL query with LIMIT 3 OFFSET 3")
    fourth_user = next(user_gen)
    user_id, name, email, age = fourth_user
    print(f"   👤 Fourth user from MySQL: {name} (Age: {age})")
    
    # Process all remaining users
    print("\n🔄 Processing remaining users with for loop:")
    user_count = 4  # We already processed 4 users
    
    for user in user_gen:
        user_count += 1
        user_id, name, email, age = user
        print(f"   👤 User {user_count}: {name}")
    
    print(f"\n🎉 Processed {user_count} users total from MySQL!")
    print("💡 Notice: Generator automatically managed MySQL queries and memory!")
    
    connection.close()


def test_database_connection():
    """
    Tests the MySQL database connection and shows sample data
    """
    print("\n" + "="*70)
    print("🧪 MYSQL DATABASE TEST")
    print("="*70)
    
    # Test connection to main server
    print("\n1. Testing MySQL server connection...")
    main_connection = connect_db()
    if not main_connection:
        print("❌ Cannot connect to MySQL server")
        return False
    
    # Create database
    create_database(main_connection)
    main_connection.close()
    
    # Test connection to specific database
    print("\n2. Testing ALX_prodev database connection...")
    prodev_connection = connect_to_prodev()
    if not prodev_connection:
        print("❌ Cannot connect to ALX_prodev database")
        return False
    
    # Create table
    create_table(prodev_connection)
    
    # Insert data
    insert_data(prodev_connection, 'user_data.csv')
    
    # Show sample data
    cursor = prodev_connection.cursor()
    cursor.execute("SELECT * FROM user_data LIMIT 5")
    sample_rows = cursor.fetchall()
    
    print("\n📊 Sample data from MySQL user_data table:")
    print("   " + "-" * 60)
    for row in sample_rows:
        user_id, name, email, age = row
        print(f"   👤 {name:25} | {email:30} | Age: {age:3}")
    print("   " + "-" * 60)
    
    # Show table info
    cursor.execute("""
        SELECT COUNT(*) as total_users, 
               AVG(age) as avg_age,
               MIN(age) as min_age, 
               MAX(age) as max_age 
        FROM user_data
    """)
    stats = cursor.fetchone()
    total_users, avg_age, min_age, max_age = stats
    
    print(f"\n📈 Database Statistics:")
    print(f"   Total Users: {total_users}")
    print(f"   Average Age: {avg_age:.1f}")
    print(f"   Age Range: {min_age} - {max_age} years")
    
    cursor.close()
    prodev_connection.close()
    
    return True


def main():
    """
    Main function that runs the complete MySQL + Generators demonstration
    """
    print("🚀 ALX Python Generators - MySQL Database Streaming")
    print("="*70)
    print("🗄️  Using MySQL Database (Not SQLite)")
    print("🎯 Learning: How generators stream data efficiently")
    print("="*70)
    
    # Step 1: Test MySQL connection and setup
    print("\n📍 STEP 1: MySQL Database Setup")
    if not test_database_connection():
        print("\n💡 TROUBLESHOOTING TIPS:")
        print("   • Ensure MySQL service is running")
        print("   • Check if MySQL is installed correctly")
        print("   • Try common passwords: blank, 'password', 'root', '123456'")
        print("   • Use MySQL Workbench to test your connection")
        return
    
    # Step 2: Demonstrate generator magic
    print("\n📍 STEP 2: Generator Magic Demonstration")
    demonstrate_generator_magic()
    
    # Step 3: Show practical generator usage
    print("\n📍 STEP 3: Practical Generator Usage Example")
    print("\n💼 Example: Processing users for email campaign")
    
    connection = connect_to_prodev()
    if connection:
        email_count = 0
        print("📧 Sending emails to users (using generator):")
        
        for user in user_data_generator(connection, batch_size=4):
            user_id, name, email, age = user
            email_count += 1
            print(f"   ✉️  Sent email {email_count}: To {name} at {email}")
            
            # Simulate some processing
            if email_count >= 8:  # Limit for demo
                print(f"   🛑 Stopped after {email_count} emails (demo limit)")
                break
        
        connection.close()
    
    print("\n" + "="*70)
    print("🎓 LEARNING OBJECTIVES ACHIEVED!")
    print("✅ MySQL Database Connection: ESTABLISHED")
    print("✅ Table Creation & Data Insertion: COMPLETED")
    print("✅ Generator Function Implementation: SUCCESSFUL")
    print("✅ Memory-Efficient Data Streaming: DEMONSTRATED")
    print("="*70)
    
    print(f"\n💡 Key Takeaway:")
    print("   Generators let you process LARGE MySQL datasets")
    print("   without loading everything into memory at once!")
    print("   This is crucial for production applications. 🚀")


if __name__ == "__main__":
    main()