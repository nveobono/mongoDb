from bson.int64 import Int64
from bson.son import SON
from pymongo import MongoClient

no_volar = MongoClient("mongodb://localhost:27017/")
database = no_volar["GRUPO5"]
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
            u"superheroes_power_matrix.Flight": False,
            u"characters_stats.Intelligence": {
                u"$gt": Int64(50)
            }
        }
    },
    {
        u"$sort": SON([ (u"characters_stats.Intelligence", -1) ])
    },
    {
        u"$project": {
            u"superheroes_power_matrix.Name": u"$superheroes_power_matrix.Name",
            u"characters_stats.Intelligence": u"$characters_stats.Intelligence",
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
    no_volar.close()
