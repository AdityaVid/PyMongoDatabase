import pymongo
import json
import sys
import time
import os

def main(): 
    """
    Main function to connect to MongoDB and insert data from a JSON file in batches.
    """
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Invalid port number provided. Please provide a valid integer port number.")
            sys.exit(1)
    else:
        print("No port number provided. Defaulting to port 27017.")
        port = 27017

    with open("config.py", "w") as f:
        f.write(f"MONGODB_PORT = {port}")

    try:
        # Connect to the MongoDB server
        client = pymongo.MongoClient(f"mongodb://localhost:{port}")
        print("Connected to the MongoDB server.")
    except ConnectionError:
        print("Failed to connect to the MongoDB server.")
        return

    database_name = 'MP2Norm'

    # Check if the desired database name exists
    if database_name in client.list_database_names():
        print(f"The database '{database_name}' already exists.")
    else:
        # Create the database if it doesn't exist
        db = client[database_name]
        print(f"Database '{database_name}' created.")

    db = client[database_name]

    # Drop the "messages" collection if it exists and then create a new collection
    if "messages" in db.list_collection_names():
        print("Dropping the existing collection 'messages'.")
        db.drop_collection("messages")

    # Create a new collection named "messages"
    db.create_collection("messages")
    print("Collection 'messages' created.")

    # Get the current working directory
    current_directory = os.getcwd()

    # Relative path to the messages.json and sender.json file
    messages_file_path = os.path.join(current_directory, "messages.json")
    senders_file_path = os.path.join(current_directory, "senders.json")

    batch_size = 7500  # Adjust this value as needed
    start_time = time.time()
    with open(messages_file_path, "r", encoding="utf-8") as json_file:
        
        data = json.load(json_file)
        batch = []
        for idx, document in enumerate(data):
            batch.append(document)
            if len(batch) == batch_size or idx == len(data) - 1:
                # Insert the batch into the MongoDB collection
                db.messages.insert_many(batch)
                batch = []
    
    end_time = time.time()

    print("All data inserted successfully.")
    elapsed_time = end_time - start_time
    print(f"Time taken to read data and create the collection: {elapsed_time} seconds")

    # Drop the "senders" collection if it exists and then create a new collection
    if "senders" in db.list_collection_names():
        print("Dropping the existing collection 'senders'.")
        db.drop_collection("senders")

    # Create a new collection named "senders"
    db.create_collection("senders")
    print("Collection 'senders' created.")

    batch_size = 5000  # Adjust this value as needed
    start_time = time.time()
    with open(senders_file_path, "r", encoding="utf-8") as json_file:
        
        data = json.load(json_file)
        batch = []
        for idx, document in enumerate(data):
            batch.append(document)
            if len(batch) == batch_size or idx == len(data) - 1:
                # Insert the batch into the MongoDB collection
                db.senders.insert_many(batch)
                batch = []
    
    end_time = time.time()  # Record end time
    print("All data inserted successfully.")
    elapsed_time = end_time - start_time
    print(f"Time taken to read data and create the collection: {elapsed_time} seconds")

    

if __name__ == "__main__":
    main()
