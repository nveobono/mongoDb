#conocer el nombre, genero, la alineación, inteligencia y poder de todos los personajes
from pymongo import MongoClient

habilidades_super = MongoClient("mongodb://localhost:27017/")
database = habilidades_super["GRUPO5"]
collection = database["characters_info"]

pipeline = [
    {
        u"$project": {
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
        u"$project": {
            u"Name": u"$characters_info.Name",
            u"Alignment": u"$characters_info.Alignment",
            u"Gender": u"$characters_info.Gender",
            u"Power": u"$characters_stats.Power",
            u"Intelligence": u"$characters_stats.Intelligence",
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
    habilidades_super.close()