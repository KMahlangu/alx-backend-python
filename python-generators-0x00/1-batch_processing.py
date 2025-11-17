#!/usr/bin/python3
"""
Batch Processing Large Data
Generator that fetches and processes data in batches
"""
import pymysql

def stream_users_in_batches(batch_size):
    """
    Generator that fetches rows from database in batches
    
    Returns:
        int: Total number of batches processed
    """
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='#Thando@2019',
        database='ALX_prodev',
        cursorclass=pymysql.cursors.DictCursor  # This returns dictionaries directly
    )
    cursor = conn.cursor()
    
    offset = 0
    batch_count = 0
    
    # LOOP 1: Batch fetching loop
    while True:
        cursor.execute("SELECT user_id, name, email, age FROM user_data LIMIT %s OFFSET %s", 
                      (batch_size, offset))
        rows = cursor.fetchall()
        
        if not rows:
            cursor.close()
            conn.close()
            return batch_count  # Return total batch count
        
        # With pymysql DictCursor, rows are already dictionaries!
        # No need to convert manually
        yield rows  # Yield the batch of dictionaries directly
        
        offset += batch_size
        batch_count += 1

def batch_processing(batch_size):
    """
    Processes batches to filter users over age 25
    
    Returns:
        dict: Processing statistics
    """
    stats = {
        'total_processed': 0,
        'total_filtered': 0,
        'batch_size': batch_size
    }
    
    # Get batches from generator
    for batch in stream_users_in_batches(batch_size):  # LOOP 3: Process batches
        stats['total_processed'] += len(batch)
        
        # Filter and print users over 25
        for user in batch:  # Each user is already a dictionary
            if user['age'] > 25:
                print(user)
                stats['total_filtered'] += 1
    
    return stats  # Return processing statistics
if __name__ == "__main__":
    batch_size = 14
    stats = batch_processing(batch_size)
    
    print("\nBatch Processing Complete!")
    print(f"Total Records Processed: {stats['total_processed']}")
    print(f"Total Records Filtered (age > 25): {stats['total_filtered']}")
    print(f"Batch Size: {stats['total_processed']}")