//todos los comics donde aparece Captain America y thor
db.getCollection("characters").aggregate(
    [
        { 
            "$project" : { 
                "characters" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters.characterID", 
                "from" : "charactersToComics", 
                "foreignField" : "characterID", 
                "as" : "charactersToComics"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$charactersToComics", 
            }
        }, 
        { 
            "$match" : { 
                "$or" : [
                    { 
                        "characters.name" : "Captain America"
                    }, 
                    { 
                        "characters.name" : "Thor"
                    }
                ]
            }
        }
    ] 
).pretty()

//numero de comics en las que aparece iron man
db.getCollection("characters").aggregate(
    [
        { 
            "$project" : { 
                "characters" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters.characterID", 
                "from" : "charactersToComics", 
                "foreignField" : "characterID", 
                "as" : "charactersToComics"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$charactersToComics", 
            }
        }, 
        { 
            "$match" : { 
                "characters.name" : "Iron Man"
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 

                }, 
                "COUNT(charactersToComics᎐comicID)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "numVeces" : "$COUNT(charactersToComics᎐comicID)", 
            }
        }
    ]
).pretty()

//conocer el nombre, genero, la alineación, inteligencia y poder de todos los personajes
db.getCollection("characters_info").aggregate(
    [
        { 
            "$project" : { 
                "characters_info" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters_info.Name", 
                "from" : "characters_stats", 
                "foreignField" : "Name", 
                "as" : "characters_stats"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$characters_stats"
            }
        }, 
        { 
            "$project" : { 
                "Name" : "$characters_info.Name", 
                "Alignment" : "$characters_info.Alignment", 
                "Gender" : "$characters_info.Gender", 
                "Power" : "$characters_stats.Power", 
                "Intelligence" : "$characters_stats.Intelligence", 
            }
        }
    ]
).pretty()

//conocer nombre de personajes  malos y  humanos con inteligencia mayor que 50 y poder superior a 20
db.getCollection("characters_info").aggregate(
    [
        { 
            "$project" : { 
                "characters_info" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters_info.Name", 
                "from" : "characters_stats", 
                "foreignField" : "Name", 
                "as" : "characters_stats"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$characters_stats"
            }
        }, 
        { 
            "$match" : { 
                "$or" : [
                    { 
                        "$and" : [
                            { 
                                "characters_info.Alignment" : "bad"
                            }, 
                            { 
                                "characters_info.Race" : "Human"
                            }
                        ]
                    }, 
                    { 
                        "$and" : [
                            { 
                                "characters_stats.Power" : { 
                                    "$gt" : NumberLong(20)
                                }
                            }, 
                            { 
                                "characters_stats.Intelligence" : { 
                                    "$gt" : NumberLong(50)
                                }
                            }
                        ]
                    }
                ]
            }
        }, 
        { 
            "$project" : { 
                "Name" : "$characters_info.Name", 
            }
        }
    ]
).pretty()

// media de velocidad de todos los personajes vivos de Marvel
db.getCollection("characters_stats").aggregate(
    [
        { 
            "$project" : { 
                "characters_stats" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters_stats.non_existing_field", 
                "from" : "marvel_dc_characters", 
                "foreignField" : "non_existing_field", 
                "as" : "marvel_dc_characters"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$marvel_dc_characters"
            }
        }, 
        { 
            "$match" : { 
                "marvel_dc_characters.Universe" : "Marvel", 
                "marvel_dc_characters.Status" : "Living"
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 

                }, 
                "AVG(characters_stats᎐Speed)" : { 
                    "$avg" : "$characters_stats.Speed"
                }
            }
        }, 
        { 
            "$project" : { 
                "media_mavel" : "$AVG(characters_stats᎐Speed)", 

            }
        }
    ]
).pretty()

//NUMERO DE PERSONAJES COLVOS CON VELOCIDAD MAYOR A 40
db.getCollection("characters_stats").aggregate(
    [
        { 
            "$project" : {  
                "characters_stats" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters_stats.non_existing_field", 
                "from" : "characters_info", 
                "foreignField" : "non_existing_field", 
                "as" : "characters_info"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$characters_info"
            }
        }, 
        { 
            "$match" : { 
                "characters_stats.Speed" : { 
                    "$gt" : NumberLong(40)
                }, 
                "characters_info.HairColor" : { 
                    "$ne" : ""
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 

                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "mayor_cuarenta" : "$COUNT(*)"
            }
        }
    ]
).pretty()

//editorial con más personales 
db.getCollection("characters_info").aggregate(
    [
        { 
            "$group" : { 
                "_id" : { 

                }, 
                "MAX(Publisher)" : { 
                    "$max" : "$Publisher"
                }
            }
        }, 
        { 
            "$project" : { 
                "MAX(Publisher)" : "$MAX(Publisher)", 
            }
        }
    ]
).pretty()

//nombre de los personajes inteligencia mayor que 50 que no puedan volar ordenados por su inteligencia
db.getCollection("superheroes_power_matrix").aggregate(
    [
        { 
            "$project" : { 
                "superheroes_power_matrix" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "superheroes_power_matrix.Name", 
                "from" : "characters_stats", 
                "foreignField" : "Name", 
                "as" : "characters_stats"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$characters_stats"
            }
        }, 
        { 
            "$match" : { 
                "superheroes_power_matrix.Flight" : false, 
                "characters_stats.Intelligence" : { 
                    "$gt" : NumberLong(50)
                }
            }
        }, 
        { 
            "$sort" : { 
                "characters_stats.Intelligence" : NumberInt(-1)
            }
        }, 
        { 
            "$project" : { 
                "superheroes_power_matrix.Name" : "$superheroes_power_matrix.Name", 
                "characters_stats.Intelligence" : "$characters_stats.Intelligence", 
            }
        }
    ]
).pretty()

//titulo de todos los comics en los que aparecen personajes personajes de mavel 


//conocer el universo y numemero de personajes de cada universo que han aparecido mas de 10 veces
db.getCollection("marvel_dc_characters").aggregate(
    [
        { 
            "$match" : { 
                "Appearances" : { 
                    "$gt" : NumberLong(10)
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "Universe" : "$Universe"
                }, 
                "COUNT(Name)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "Universe" : "$_id.Universe", 
                "COUNT(Name)" : "$COUNT(Name)", 
                "Appearances" : "$_id.Appearances", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$sort" : { 
                "Appearances" : NumberInt(1)
            }
        }, 
        { 
            "$project" : { 
                "_id" : NumberInt(0), 
                "Universe" : "$Universe", 
                "COUNT(Name)" : "$COUNT(Name)"
            }
        }
    ]
).pretty()

//personajes femeninos longevos no malas con una inteligeria y poder mayor que 50
db.getCollection("superheroes_power_matrix").aggregate(
    [
        { 
            "$project" : {  
                "superheroes_power_matrix" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "superheroes_power_matrix.Name", 
                "from" : "characters_info", 
                "foreignField" : "Name", 
                "as" : "characters_info"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$characters_info"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "characters_info.Name", 
                "from" : "characters_stats", 
                "foreignField" : "Name", 
                "as" : "characters_stats"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$characters_stats"
            }
        }, 
        { 
            "$match" : { 
                "characters_info.Gender" : "Female", 
                "superheroes_power_matrix.Longevity" : true, 
                "characters_info.Alignment" : { 
                    "$ne" : "bad"
                }, 
                "characters_stats.Intelligence" : { 
                    "$gt" : NumberLong(50)
                }, 
                "characters_stats.Power" : { 
                    "$gt" : NumberLong(50)
                }
            }
        }
    ]
).pretty()

