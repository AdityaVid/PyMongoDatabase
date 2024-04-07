import json
import pymongo
import sys
import time
import os

def main():
    """
    Main function to connect to MongoDB and insert data from JSON files in batches.
    """
    # Check if the port number is provided as a command-line argument
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Invalid port number provided. Please provide a valid integer port number.")
            sys.exit(1)
    else:
        print("No port number provided. Defaulting to port 27017.")
        port = 27017

    with open("config1.py", "w") as f:
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

    # Get the current working directory
    current_directory = os.getcwd()

    # Relative path to the messages.json and sender.json file
    messages_file_path = os.path.join(current_directory, "messages.json")
    senders_file_path = os.path.join(current_directory, "senders.json")

    if "senders" in db.list_collection_names():
        print("Dropping the existing collection 'senders'.")
        db.drop_collection("senders")

    # Create a new collection named "messages"
    db.create_collection("senders")
    print("Collection 'senders' created.")

    # Read "senders.json" file and load it into memory
    senders_data = None
    with open(senders_file_path, "r", encoding="utf-8") as senders_file:
        senders_data = json.load(senders_file)

    # Now, senders_data contains the JSON data loaded into memory
    print("senders.json file loaded into memory.")

    # Drop the "messages" collection if it exists and then create a new collection
    if "messages" in db.list_collection_names():
        print("Dropping the existing collection 'messages'.")
        db.drop_collection("messages")

    # Create a new collection named "messages"
    db.create_collection("messages")
    print("Collection 'messages' created.")

    batch_size = 7500  # Adjust this value as needed
    start_time = time.time()
    with open(messages_file_path, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)
        batch = []
        for idx, document in enumerate(data):
            # Find the corresponding sender info
            sender_id = document["sender"]
            sender_info = next((sender for sender in senders_data if sender["sender_id"] == sender_id), None)
            if sender_info:
                # Embed sender_info into the document
                document["sender_info"] = sender_info
                batch.append(document)
            else:
                print(f"Sender info not found for sender_id: {sender_id}")
                
            if len(batch) == batch_size or idx == len(data) - 1:
                # Insert the batch into the MongoDB collection
                db.messages.insert_many(batch)
                batch = []

    end_time = time.time()

    print("All data inserted successfully.")
    elapsed_time = end_time - start_time
    print(f"Time taken to read data, embed sender_info, and create the collection: {elapsed_time} seconds")

if __name__ == "__main__":
    main()
