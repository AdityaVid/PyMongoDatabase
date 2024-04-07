import pymongo
import config
import time
from pymongo.errors import PyMongoError
# Access the port value
port = config.MONGODB_PORT

def main():

    client = pymongo.MongoClient(f"mongodb://localhost:{port}/")

    # Access the database
    db = client['MP2Norm']

    # Access the collections
    messages_collection = db["messages"]
    senders_collection = db["senders"]

    # Create index for "sender" field using default index type
    messages_collection.create_index("sender")

    # Create text index for "text" field
    messages_collection.create_index([("text", pymongo.TEXT)])

    # Create index for "sender_id" field
    senders_collection.create_index("sender_id")

    try:
        start_time = time.time()
        pipeline = [
        {
            "$match": {
                "text": {"$regex": "ant", "$options": "i"}  # Case-insensitive regex search for "ant"
            }
        },
        {
            "$count": "total_messages_with_ant"
        }
        ]

        result = list(messages_collection.aggregate(pipeline, maxTimeMS = 120000))

        count = result[0]["total_messages_with_ant"] if result else 0
        print("Q1 Result:")
        print("Number of messages with 'ant' in text:", count)
        print("Q1 execution time:", time.time() - start_time)

    except PyMongoError as exc:
        if isinstance(exc, pymongo.errors.ExecutionTimeout):
            print("Query 1 took more than two minutes to execute.")
    
    try:
        start_time = time.time()
        # Q2: Find the nickname/phone number of the sender who has sent the greatest number of messages.
        # Return the nickname/phone number and the number of messages sent by that sender.
        pipeline = [
            {
                "$group": {
                    "_id": "$sender",
                    "total_messages": {"$sum": 1}
                }
            },
            {
                "$sort": {"total_messages": -1}
            },
            {
                "$limit": 1
            },
            {
                "$project": {
                    "_id": 0,
                    "sender": "$_id",
                    "total_messages": 1
                }
            }
        ]

        result = list(messages_collection.aggregate(pipeline, maxTimeMS = 120000))

        if result:
            sender_with_most_messages = result[0]
            print("Sender with the most messages:", sender_with_most_messages)
        else:
            print("No messages found.")
        
        print("Q2 execution time:", time.time() - start_time)

    except PyMongoError as exc:
        if isinstance(exc, pymongo.errors.ExecutionTimeout):
            print("Query 2 took more than two minutes to execute.")

    try:
        start_time = time.time()
        # Q3: Return the number of messages where the sender’s credit is 0.
        sender_ids_with_zero_credit = senders_collection.distinct("sender_id", {"credit": 0})

        # Then, use those sender IDs to find messages where sender is in the filtered list
        pipeline = [
            {
                "$match": {
                    "sender": {"$in": sender_ids_with_zero_credit}
                }
            },
            {
                "$count": "total_messages"
            }
        ]

        result = list(messages_collection.aggregate(pipeline, maxTimeMS = 120000))
        print("Q3 Result: ")
        if result:
            total_messages = result[0]["total_messages"]
            print("Number of Messages where Sender's Credit is 0:", total_messages)
        else:
            print("No messages found where Sender's Credit is 0.")
        print("Q3 execution time:", time.time() - start_time)

    except PyMongoError as exc:
        if isinstance(exc, pymongo.errors.ExecutionTimeout):
            print("Query 3 took more than two minutes to execute.")
    

    try:
        start_time = time.time()
        # Q4: Double the credit of all senders whose credit is less than 100.
        senders_collection.update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}})
        print("Q4: Credits doubled for senders with credit less than 100.")
        print("Q4 execution time:", time.time() - start_time)

    except PyMongoError as exc:
        if isinstance(exc, pymongo.errors.ExecutionTimeout):
            print("Query 4 took more than two minutes to execute.")



if __name__ == "__main__":
    main()