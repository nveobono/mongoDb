#conocer nombre de personajes  malos y  humanos con inteligencia mayor que 50 y poder superior a 20
from bson.int64 import Int64
from pymongo import MongoClient

bad_humans = MongoClient("mongodb://localhost:27017/")
database = bad_humans["GRUPO5"]
collection = database["characters_info"]

pipeline = [
    {
        u"$project": {
            u"_id": 0,
            u"characters_info": u"$$ROOT"
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
            u"$or": [
                {
                    u"$and": [
                        {
                            u"characters_info.Alignment": u"bad"
                        },
                        {
                            u"characters_info.Race": u"Human"
                        }
                    ]
                },
                {
                    u"$and": [
                        {
                            u"characters_stats.Power": {
                                u"$gt": Int64(20)
                            }
                        },
                        {
                            u"characters_stats.Intelligence": {
                                u"$gt": Int64(50)
                            }
                        }
                    ]
                }
            ]
        }
    },
    {
        u"$project": {
            u"Name": u"$characters_info.Name"
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
    bad_humans.close()