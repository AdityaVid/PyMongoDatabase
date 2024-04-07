import pymongo
import config1
import time
from pymongo.errors import PyMongoError
# Access the port value
port = config1.MONGODB_PORT

def main():
    
    client = pymongo.MongoClient(f"mongodb://localhost:{port}/")

    # Access the database
    db = client['MP2Norm']

    # Access the collections
    messages_collection = db["messages"]
    senders_collection = db["senders"]

    try:
        start_time = time.time()
        # Q1: Return the number of messages that have "ant" in their text.
        # Define the filter to find messages containing "ant" in their text
        filter_query = {"text": {"$regex": "ant"}}
        # Count the number of documents matching the filter
        query1 = messages_collection.find(filter_query, max_time_ms = 120000)
        doc_count = len(list(query1))
        print("Q1 Result:")
        print("Number of messages with 'ant' in text:", doc_count)
        print("Q1 execution time:", time.time() - start_time)
    
    except PyMongoError as exc:
        if isinstance(exc, pymongo.errors.ExecutionTimeout):
            print("Query 1 took more than two minutes to execute.")
    
    try:
        start_time = time.time()

        # Aggregate to group messages by sender and count the number of messages sent by each sender
        pipeline = [
            {"$group": {"_id": "$sender", "message_count": {"$sum": 1}}},
            {"$sort": {"message_count": -1}},
            {"$limit": 1}
        ]
        result = list(messages_collection.aggregate(pipeline, maxTimeMS = 120000))
        if result:
            sender = result[0]["_id"]
            message_count = result[0]["message_count"]
            print("Q2 Result:")
            print("Sender:", sender)
            print("Number of messages sent:", message_count)
            print("Q2 execution time:", time.time() - start_time)
        else:
            print("No messages found.")
    except pymongo.errors.PyMongoError as exc:
        print("An error occurred:", exc)

    try:
        start_time = time.time()

        # Find messages where sender's credit is 0
        query = {"sender_info.credit": 0}
        zero_credit_messages_count = messages_collection.count_documents(query, maxTimeMS = 120000)

        print("Q3 Result:")
        print("Number of messages with sender's credit 0:", zero_credit_messages_count)
        print("Q3 execution time:", time.time() - start_time)
    except pymongo.errors.PyMongoError as exc:
        print("An error occurred:", exc)
    
    try:
        start_time = time.time()

        # Update operation to double the credit of senders with credit less than 100
        query = {"credit": {"$lt": 100}}
        update_operation = {"$mul": {"credit": 2}}
        result = senders_collection.update_many(query, update_operation)
        count = result.modified_count
        print("Q4 Result:")
        print("Number of senders with credit less than 100:", count)
        print("Q4 execution time:", time.time() - start_time)
    except pymongo.errors.PyMongoError as exc:
        print("An error occurred:", exc)
    

if __name__ == "__main__":
    main()