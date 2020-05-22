#editorial con más personales
from pymongo import MongoClient

mayor_editorial = MongoClient("mongodb://localhost:27017/")
database = mayor_editorial["GRUPO5"]
collection = database["characters_info"]

pipeline = [
    {
        u"$group": {
            u"_id": {},
            u"MAX(Publisher)": {
                u"$max": u"$Publisher"
            }
        }
    },
    {
        u"$project": {
            u"mayor_editorial": u"$MAX(Publisher)",
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
    mayor_editorial.close()