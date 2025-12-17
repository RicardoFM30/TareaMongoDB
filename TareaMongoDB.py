from pymongo import MongoClient
from dotenv import load_dotenv
import os
from faker import Faker
from random import randint, choice, uniform
import json

print("--- PARTE 1 ---")

# Configuración
fake = Faker("es_ES")
load_dotenv()

USUARIO = os.getenv("USUARIO_MONGODB")
PASSWORD = os.getenv("PASSWORD_MONGODB")
CLUSTER = os.getenv("CLUSTER_MONGODB")

uri = (
    f"mongodb+srv://{USUARIO}:{PASSWORD}@"
    f"{CLUSTER}.mongodb.net/"
    f"?retryWrites=true&w=majority&appName=TV_StreamDB"
)

cliente = MongoClient(uri)
cliente.admin.command("ping")
print("Conectado correctamente a MongoDB Atlas")

print("\n--- PARTE 2 ---")

db = cliente["TV_StreamDB"]
coleccion_series = db["series"]

coleccion_series.delete_many({})

# Datos auxiliares
plataformas = ["Netflix", "HBO", "Amazon Prime", "Disney+", "Apple TV+"]
generos = ["Drama", "Sci-Fi", "Acción", "Comedia", "Thriller", "Fantasía"]

# 50 series COMPLETAS
series_completas = []

for _ in range(50):
    serie = {
        "titulo": fake.sentence(nb_words=3).replace(".", ""),
        "plataforma": choice(plataformas),
        "temporadas": randint(1, 10),
        "genero": fake.random_elements(elements=generos, length=2, unique=True),
        "puntuacion": round(uniform(6.0, 9.5), 1),
        "finalizada": choice([True, False]),
        "año_estreno": randint(2000, 2026)
    }
    series_completas.append(serie)

coleccion_series.insert_many(series_completas)
print("📺 50 series completas insertadas")

# 10 series INCOMPLETAS
series_incompletas = []

campos = [
    "plataforma",
    "temporadas",
    "genero",
    "puntuacion",
    "año_estreno"
]

for _ in range(10):
    serie = {
        "titulo": fake.sentence(nb_words=3).replace(".", ""),
        "plataforma": choice(plataformas),
        "temporadas": randint(1, 10),
        "genero": fake.random_elements(elements=generos, length=2, unique=True),
        "puntuacion": round(uniform(6.0, 9.5), 1),
        "finalizada": choice([True, False]),
        "año_estreno": randint(2000, 2026)
    }

    campo_a_eliminar = choice(campos)
    serie.pop(campo_a_eliminar)

    series_incompletas.append(serie)

coleccion_series.insert_many(series_incompletas)
print(" 10 series incompletas insertadas")

# Verificación
print(" Total documentos:", coleccion_series.count_documents({}))

print("\n--- PARTE 3 ---")

# Maratones Largas
# Series con más de 5 temporadas y puntuación > 8.0
print("\n Maratones Largas:")
for serie in coleccion_series.find({"temporadas": {"$gt": 5}, "puntuacion": {"$gt": 8.0}}):
    print(serie)

# Joyas Recientes de Comedia
# Series de género Comedia estrenadas a partir de 2020
print("\n Joyas Recientes de Comedia:")
for serie in coleccion_series.find({"genero": "Comedia", "año_estreno": {"$gte": 2020}}):
    print(serie)

# Contenido Finalizado
# Series donde finalizada == True
print("\n Contenido Finalizado:")
for serie in coleccion_series.find({"finalizada": True}):
    print(serie)

# Elementos de la base de datos con el campo puntuación nulo
print("\n Elementos de la base de datos con el campo puntuación nulo:")
for serie in coleccion_series.find({"puntuacion": {"$exists": False}}):
    print(serie)

print("\n--- PARTE 4 ---")

# Carpeta para tener más ordenado los archivos
carpeta = "jsons"
if not os.path.exists(carpeta):
    os.makedirs(carpeta)
    print(f"Carpeta '{carpeta}' creada")

# Función para exportar resultados
def exportar_a_json_consultas(query, filename):
    resultados = list(coleccion_series.find(query))
    for doc in resultados:
        doc['_id'] = str(doc['_id'])  # Convertir ObjectId a string
    ruta = os.path.join(carpeta, filename)
    with open(ruta, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=4)
    print(f"Exportado {len(resultados)} documentos a {ruta}")


# Consultas

# Maratones Largas
query_maratones = {"temporadas": {"$gt": 5}, "puntuacion": {"$gt": 8.0}}
exportar_a_json_consultas(query_maratones, "maratones.json")

# Joyas Recientes de Comedia
query_comedias = {"genero": "Comedia", "año_estreno": {"$gte": 2020}}
exportar_a_json_consultas(query_comedias, "comedias_recientes.json")

# Contenido Finalizado
query_finalizadas = {"finalizada": True}
exportar_a_json_consultas(query_finalizadas, "series_finalizadas.json")

# Consulta inventada: Elementos de la base de datos con el campo puntuación nulo
query_inventada = {"puntuacion": {"$exists": False}}
exportar_a_json_consultas(query_inventada, "inventada.json")

print("\n--- PARTE 5 ---")

resultado = list(coleccion_series.aggregate([
    {"$match": {"puntuacion": {"$exists": True}}},
    {"$group": {"_id": None, "media_puntuacion": {"$avg": "$puntuacion"}}}
]))

if resultado:
    media = resultado[0]["media_puntuacion"]
    print(f"La media de puntuación de todas las series es: {media:.2f}")
else:
    print("No hay series con puntuación para calcular la media")

print("\n--- PARTE 6 ---")

coleccion_detalles = db["detalles_produccion"]

# Limpiar colección nueva
coleccion_detalles.delete_many({})

paises = ["EE.UU.", "Corea del Sur", "España", "Reino Unido", "Canadá"]

# Consulta para obtener los títulos de la colección series
titulos = [serie["titulo"] for serie in coleccion_series.find({}, {"titulo": 1})]

# Agregar a la nueva colección
detalles_list = []

for titulo in titulos:
    detalle = {
        "titulo": titulo,
        "pais_origen": choice(paises),
        "reparto_principal": [fake.name() for _ in range(3)],
        "presupuesto_por_episodio": round(uniform(1.0, 10.0), 2)
    }
    detalles_list.append(detalle)

coleccion_detalles.insert_many(detalles_list)
print(f"Insertados {len(detalles_list)} documentos en detalles_produccion")

resultados = list(coleccion_series.aggregate([
    {
        "$match": {"finalizada": True, "puntuacion": {"$gt": 8.0}}
    },
    {
        "$lookup": {
            "from": "detalles_produccion",
            "localField": "titulo",
            "foreignField": "titulo",
            "as": "detalle"
        }
    },
    {
        "$unwind": "$detalle"
    },
    {
        "$match": {"detalle.pais_origen": "EE.UU."}
    },
    {
        "$project": {
            "_id": 0,
            "titulo": 1,
            "puntuacion": 1,
            "pais_origen": "$detalle.pais_origen",
            "reparto_principal": "$detalle.reparto_principal",
            "presupuesto_por_episodio": "$detalle.presupuesto_por_episodio"
        }
    }
]))

# Mostrar resultados
print(f"\nSeries finalizadas, puntuación > 8 y país EE.UU.: {len(resultados)}")
for serie in resultados:
    print(serie)

print("\n--- PARTE 7 ---")

resultados = list(coleccion_series.aggregate([
    {
        "$lookup": {
            "from": "detalles_produccion",
            "localField": "titulo",
            "foreignField": "titulo",
            "as": "detalle"
        }
    },
    {"$unwind": "$detalle"},
    {
        "$project": {
            "_id": 0,
            "titulo": 1,
            "coste_total": {
                "$multiply": ["$detalle.presupuesto_por_episodio", "$temporadas", 8]
            }
        }
    }
]))

ruta_json = os.path.join(carpeta, "gasto_series.json")
with open(ruta_json, "w", encoding="utf-8") as f:
    json.dump(resultados, f, ensure_ascii=False, indent=4)

print(f"Se han guardado {len(resultados)} registros en {ruta_json}")