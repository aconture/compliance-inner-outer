import pymongo

# establing connection
try:
    myclient = pymongo.MongoClient("mongodb://compliance-inner-outer_mongo_1:27017/")
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")


# connecting or switching to the database
db = myclient["temporal"]
# creating or switching to collection
collection = db["core_history"]


# documents
document1 = {
        "ne": "RSC1NA",
        "ok": "63",
        "ok_reserva": "63",
        "revisar_1": "3",
        "finv": "42"
        }

document2 = {
        "ne": "ROC2.SLO1",
        "ok": "63",
        "okreserva": "63",
        "revisar1": "3",
        "finv": "42"
        }

document3 = {
        "ne": "ROC2.HOR1",
        "ok": "63",
        "okreserva": "63",
        "revisar1": "3",
        "finv": "42"
        }

# Inserting 
collection.insert_one(document1)
collection.insert_one(document2)
collection.insert_one(document3)

# Printing 
cursor = collection.find()
for record in cursor:
    print(record)

