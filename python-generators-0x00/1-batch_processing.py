#!/usr/bin/python3
"""
Batch Processing Large Data
Generator that fetches and processes data in batches
"""
import sys
import pymysql

def stream_users_in_batches(batch_size):
    """
    Generator that fetches rows from database in batches
    
    Args:
        batch_size (int): Number of rows to fetch per batch
    
    Yields:
        list: A batch of user dictionaries
    """
    try:
        # Connect to database
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='#Thando@2019',
            database='ALX_prodev'
        )
        cursor = conn.cursor()
        
        # Get total count for progress (optional)
        cursor.execute("SELECT COUNT(*) FROM user_data")
        total_users = cursor.fetchone()[0]
        print(f"Processing {total_users} users in batches of {batch_size}", file=sys.stderr)
        
        offset = 0
        
        # LOOP 1: Batch fetching loop
        while True:
            # Fetch one batch from database
            query = f"SELECT user_id, name, email, age FROM user_data LIMIT {batch_size} OFFSET {offset}"
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:  # No more data
                break
            
            # Convert batch to list of dictionaries
            batch = []
            for row in rows:  # LOOP 2: Within batch processing
                user_dict = {
                    'user_id': row[0],
                    'name': row[1],
                    'email': row[2],
                    'age': row[3]
                }
                batch.append(user_dict)
            
            # Yield the entire batch
            yield batch
            offset += batch_size
        
        cursor.close()
        conn.close()
        
    except pymysql.Error as e:
        print(f"Database error: {e}", file=sys.stderr)

def batch_processing(batch_size):
    """
    Processes batches to filter users over age 25
    
    Args:
        batch_size (int): Number of rows to process per batch
    """
    import sys
    
    # Get the batch generator
    batches_gen = stream_users_in_batches(batch_size)
    
    # LOOP 3: Process each batch
    for batch in batches_gen:
        # Filter users over age 25 in this batch
        filtered_users = [user for user in batch if user['age'] > 25]  # This is a comprehension, not a loop
        
        # Output filtered users
        for user in filtered_users:
            print(user)

if __name__ == "__main__":
    BATCH_SIZE = 100  # Define batch size
    batch_processing(BATCH_SIZE)