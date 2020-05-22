#todos los comics donde aparece Captain America y thor
from pymongo import MongoClient

captain_thor = MongoClient("mongodb://localhost:27017/")
database = captain_thor["GRUPO5"]
collection = database["characters"]

pipeline = [
    {
        u"$project": {
            u"characters": u"$$ROOT"
        }
    },
    {
        u"$lookup": {
            u"localField": u"characters.characterID",
            u"from": u"charactersToComics",
            u"foreignField": u"characterID",
            u"as": u"charactersToComics"
        }
    },
    {
        u"$unwind": {
            u"path": u"$charactersToComics"
        }
    },
    {
        u"$match": {
            u"$or": [
                {
                    u"characters.name": u"Captain America"
                },
                {
                    u"characters.name": u"Thor"
                }
            ]
        }
    }
]

cursor = collection.aggregate(
    pipeline,
)
try:
    for i in cursor:
        print(i)
finally:
    captain_thor.close()