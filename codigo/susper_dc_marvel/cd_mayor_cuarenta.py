#NUMERO DE PERSONAJES COLVOS CON VELOCIDAD MAYOR A 40
from bson.int64 import Int64
from pymongo import MongoClient

mayor_cuarenta = MongoClient("mongodb://localhost:27017/")
database = mayor_cuarenta["GRUPO5"]
collection = database["characters_stats"]


pipeline = [
    {
        u"$project": {
            u"characters_stats": u"$$ROOT"
        }
    },
    {
        u"$lookup": {
            u"localField": u"characters_stats.non_existing_field",
            u"from": u"characters_info",
            u"foreignField": u"non_existing_field",
            u"as": u"characters_info"
        }
    },
    {
        u"$unwind": {
            u"path": u"$characters_info"
        }
    },
    {
        u"$match": {
            u"characters_stats.Speed": {
                u"$gt": Int64(40)
            },
            u"characters_info.HairColor": {
                u"$ne": u""
            }
        }
    },
    {
        u"$group": {
            u"_id": {},
            u"COUNT(*)": {
                u"$sum": 1
            }
        }
    },
    {
        u"$project": {
            u"mayor_cuarenta": u"$COUNT(*)",
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
    mayor_cuarenta.close()