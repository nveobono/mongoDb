#numero de comics en las que aparece iron man

from pymongo import MongoClient

iron_man = MongoClient("mongodb://localhost:27017/")
database = iron_man["GRUPO5"]
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
            u"path": u"$charactersToComics",
        }
    },
    {
        u"$match": {
            u"characters.name": u"Iron Man"
        }
    },
    {
        u"$group": {
            u"_id": {},
            u"COUNT(charactersToComics\u1390comicID)": {
                u"$sum": 1
            }
        }
    },
    {
        u"$project": {
            u"numVeces_iron_man": u"$COUNT(charactersToComics\u1390comicID)",
            u"_id": 0
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
    iron_man.close()