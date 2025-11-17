#!/usr/bin/python3
import pymysql
import pymysql.cursors

def stream_users():
    """Generator that streams rows from the user_data table one by one."""
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="#Thando@2019",
        database="ALX_prodev",
        cursorclass=pymysql.cursors.DictCursor
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM user_data")

    for row in cursor:
        yield row

    cursor.close()
    conn.close()
def main():
    """Main function to demonstrate streaming users from the database."""
    print("Streaming users from the database:")
    for user in stream_users():
        user['age'] >= 25
        print(user)
        
    print("\nThis approach allows processing large datasets ")
    print("   one record at a time, ")

if __name__ == "__main__":
    main()



   