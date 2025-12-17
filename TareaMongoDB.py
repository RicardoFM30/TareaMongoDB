from pymongo import MongoClient
from dotenv import load_dotenv
import os
from faker import Faker
from random import randint, choice, uniform

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
        "año_estreno": randint(1995, 2024)
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
        "año_estreno": randint(1995, 2024)
    }

    campo_a_eliminar = choice(campos)
    serie.pop(campo_a_eliminar)

    series_incompletas.append(serie)

coleccion.insert_many(series_incompletas)
print("⚠️ 10 series incompletas insertadas")

# Verificación
print("📊 Total documentos:", coleccion.count_documents({}))
