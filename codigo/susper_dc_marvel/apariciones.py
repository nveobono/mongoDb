from bson.int64 import Int64
from bson.son import SON
from pymongo import MongoClient

apariciones = MongoClient("mongodb://localhost:27017/")
database = apariciones["GRUPO5"]
collection = database["marvel_dc_characters"]

pipeline = [
    {
        u"$match": {
            u"Appearances": {
                u"$gt": Int64(10)
            }
        }
    },
    {
        u"$group": {
            u"_id": {
                u"Universe": u"$Universe"
            },
            u"COUNT(Name)": {
                u"$sum": 1
            }
        }
    },
    {
        u"$project": {
            u"Universe": u"$_id.Universe",
            u"COUNT(Name)": u"$COUNT(Name)",
            u"Appearances": u"$_id.Appearances",
        }
    },
    {
        u"$sort": SON([ (u"Appearances", 1) ])
    },
    {
        u"$project": {
            u"Universo": u"$Universe",
            u"numero personajes": u"$COUNT(Name)"
        }
    }
]

cursor = collection.aggregate(
    pipeline
)
try:
    for i in cursor:
        print(i)
finally:
    apariciones.close()
