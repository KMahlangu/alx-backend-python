#!/usr/bin/env python3
from itertools import islice
from stream_users import stream_users
  # Import the function, not the module

if __name__ == "__main__":
    for user in islice(stream_users(), 6):
        user['age'] >= 25
        print(user)
        