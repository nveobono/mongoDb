from bson.int64 import Int64
from pymongo import MongoClient

femenina_long = MongoClient("mongodb://localhost:27017/")
database = femenina_long["GRUPO5"]
collection = database["superheroes_power_matrix"]

pipeline = [
    {
        u"$project": {
            u"superheroes_power_matrix": u"$$ROOT"
        }
    },
    {
        u"$lookup": {
            u"localField": u"superheroes_power_matrix.Name",
            u"from": u"characters_info",
            u"foreignField": u"Name",
            u"as": u"characters_info"
        }
    },
    {
        u"$unwind": {
            u"path": u"$characters_info"
        }
    },
    {
        u"$lookup": {
            u"localField": u"characters_info.Name",
            u"from": u"characters_stats",
            u"foreignField": u"Name",
            u"as": u"characters_stats"
        }
    },
    {
        u"$unwind": {
            u"path": u"$characters_stats"
        }
    },
    {
        u"$match": {
            u"characters_info.Gender": u"Female",
            u"superheroes_power_matrix.Longevity": True,
            u"characters_info.Alignment": {
                u"$ne": u"bad"
            },
            u"characters_stats.Intelligence": {
                u"$gt": Int64(50)
            },
            u"characters_stats.Power": {
                u"$gt": Int64(50)
            }
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
    femenina_long.close()