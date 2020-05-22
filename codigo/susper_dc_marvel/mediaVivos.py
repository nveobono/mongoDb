#media de velocidad de todos los personajes vivos de Marvel
from pymongo import MongoClient

media_marvel = MongoClient("mongodb://localhost:27017/")
database = media_marvel["GRUPO5"]
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
            u"from": u"marvel_dc_characters",
            u"foreignField": u"non_existing_field",
            u"as": u"marvel_dc_characters"
        }
    },
    {
        u"$unwind": {
            u"path": u"$marvel_dc_characters"
        }
    },
    {
        u"$match": {
            u"marvel_dc_characters.Universe": u"Marvel",
            u"marvel_dc_characters.Status": u"Living"
        }
    },
    {
        u"$group": {
            u"_id": {},
            u"AVG(characters_stats\u1390Speed)": {
                u"$avg": u"$characters_stats.Speed"
            }
        }
    },
    {
        u"$project": {
            u"media_marvel": u"$AVG(characters_stats\u1390Speed)",
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
    media_marvel.close()