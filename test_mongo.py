from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client.test_database  # or your database name
collection = db.test_collection  # or your collection name

result = collection.insert_one({"name": "Devin", "role": "CS Major"})
print(f"Inserted document ID: {result.inserted_id}")
