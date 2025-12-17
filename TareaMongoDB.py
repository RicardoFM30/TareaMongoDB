from pymongo import MongoClient
from dotenv import load_dotenv
import os
from faker import Faker
from random import randint, choice, uniform
import json

# PARTE 1

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
print("✅ Conectado a MongoDB Atlas")

# PARTE 2

db = cliente["TV_StreamDB"]
coleccion = db["series"]

coleccion.delete_many({})

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

coleccion.insert_many(series_completas)
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

coleccion.insert_many(series_incompletas)
print(" 10 series incompletas insertadas")

# Verificación
print(" Total documentos:", coleccion.count_documents({}))

# PARTE 3

# Maratones Largas
# Series con más de 5 temporadas y puntuación > 8.0
print("\n Maratones Largas:")
for serie in coleccion.find({"temporadas": {"$gt": 5}, "puntuacion": {"$gt": 8.0}}):
    print(serie)

# Joyas Recientes de Comedia
# Series de género Comedia estrenadas a partir de 2020
print("\n Joyas Recientes de Comedia:")
for serie in coleccion.find({"genero": "Comedia", "año_estreno": {"$gte": 2020}}):
    print(serie)

# Contenido Finalizado
# Series donde finalizada == True
print("\n Contenido Finalizado:")
for serie in coleccion.find({"finalizada": True}):
    print(serie)

# Elementos de la base de datos con el campo puntuación nulo
print("\n Elementos de la base de datos con el campo puntuación nulo:")
for serie in coleccion.find({"puntuacion": {"$exists": False}}):
    print(serie)

# PARTE 4

# Carpeta para tener más ordenado los archivos
carpeta = "jsons"
if not os.path.exists(carpeta):
    os.makedirs(carpeta)
    print(f"Carpeta '{carpeta}' creada")

# Función para exportar resultados
def exportar_a_json_consultas(query, filename):
    resultados = list(coleccion.find(query))
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
