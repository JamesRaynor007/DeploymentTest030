from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import pandas as pd
import os

# Define the paths for the CSV files
file_path_monthly = os.path.join(os.path.dirname(__file__), 'PeliculasPorMesListo.csv')
file_path_daily = os.path.join(os.path.dirname(__file__), 'PeliculasPorDiaListo.csv')
votes_file_path = os.path.join(os.path.dirname(__file__), 'FuncionVotos.csv')
scores_file_path = os.path.join(os.path.dirname(__file__), 'FuncionScore.csv')

# Create a dictionary to map Spanish months to English months
meses_map = {
    'enero': 'January',
    'febrero': 'February',
    'marzo': 'March',
    'abril': 'April',
    'mayo': 'May',
    'junio': 'June',
    'julio': 'July',
    'agosto': 'August',
    'septiembre': 'September',
    'octubre': 'October',
    'noviembre': 'November',
    'diciembre': 'December'
}

# Create a dictionary to map Spanish days to English days
dias_map = {
    'lunes': 'Monday',
    'martes': 'Tuesday',
    'miercoles': 'Wednesday',
    'jueves': 'Thursday',
    'viernes': 'Friday',
    'sabado': 'Saturday',
    'domingo': 'Sunday',
}

app = FastAPI(
    title="API de Películas",
    description="Esta API permite consultar información sobre películas, sus votaciones y puntuaciones.",
    version="1.0.0",
)

class MessageResponse(BaseModel):
    mensaje: str  # Mensaje personalizado

# Load datasets
try:
    df_monthly = pd.read_csv(file_path_monthly)
    df_daily = pd.read_csv(file_path_daily)
    votes_df = pd.read_csv(votes_file_path)
    scores_df = pd.read_csv(scores_file_path)
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Error al cargar los archivos: {str(e)}")

# Ensure required columns are present
for df, required_columns in [
    (df_monthly, ['title', 'month']),
    (df_daily, ['title', 'day_of_week']),
    (votes_df, ['title', 'vote_count', 'vote_average']),
    (scores_df, ['title', 'release_year', 'popularity'])
]:
    if not all(column in df.columns for column in required_columns):
        raise HTTPException(status_code=500, detail="El DataFrame no contiene las columnas esperadas.")

# Convert columns to lowercase
df_monthly.columns = df_monthly.columns.str.lower()
df_daily.columns = df_daily.columns.str.lower()
votes_df.columns = votes_df.columns.str.lower()
scores_df.columns = scores_df.columns.str.lower()

@app.get("/", response_model=dict)
async def read_root(request: Request):
    base_url = str(request.url).rstrip('/')
    return {
        "Mensaje": "Bienvenido a la API de películas.",
        "Instrucciones": (
            "Utiliza los siguientes endpoints para interactuar con la API:",
            "/peliculas/mes/?mes=nombre_del_mes",
            "/peliculas/dia/?dia=nombre_del_dia",
            "/votes/?title=nombre_pelicula",
            "/score/?title=nombre_pelicula",
            "/titles/"
        ),
        "Links Ejemplo": [
            {"Para Mes": list(meses_map.keys())[0], "url": f"{base_url}/peliculas/mes/?mes={list(meses_map.keys())[0]}"},
            {"Para Dia": list(dias_map.keys())[0], "url": f"{base_url}/peliculas/dia/?dia={list(dias_map.keys())[0]}"},
            {"Para Votación": f"{base_url}/votes/?title=Inception", "Descripción": "Buscar votación de una película"},
            {"Para Puntuación": f"{base_url}/score/?title=Toy%20Story", "Descripción": "Buscar puntuación de una película"},
            {"Para Títulos": f"{base_url}/titles/", "Descripción": "Listar todos los títulos"}
        ]
    }

@app.get("/peliculas/mes/", response_model=MessageResponse)
def get_peliculas_mes(mes: str):
    mes = mes.lower()
    if mes not in meses_map:
        raise HTTPException(status_code=400, detail="Mes no válido. Por favor ingrese un mes en español.")

    mes_en_ingles = meses_map[mes]
    resultado = df_monthly[df_monthly['month'] == mes_en_ingles]
    cantidad = resultado['title'].count() if not resultado.empty else 0

    return MessageResponse(
        mensaje=f"Cantidad de películas que fueron estrenadas en el mes de {mes_en_ingles}: {cantidad}"
    )

@app.get("/peliculas/dia/", response_model=MessageResponse)
def get_peliculas_dia(dia: str):
    dia = dia.lower()
    if dia not in dias_map:
        raise HTTPException(status_code=400, detail="Día no válido. Por favor ingrese un día en español.")

    dia_en_ingles = dias_map[dia]
    cantidad = df_daily[df_daily['day_of_week'] == dia_en_ingles].shape[0]

    return MessageResponse(
        mensaje=f"Cantidad de películas que fueron estrenadas en el día {dia_en_ingles}: {cantidad}"
    )

@app.get("/votes/")
async def get_movie_votes(title: str):
    movie = votes_df[votes_df['title'].str.lower() == title.lower()]
    if movie.empty:
        raise HTTPException(status_code=404, detail="Película no encontrada.")
    
    movie_data = movie.iloc[0]
    if movie_data['vote_count'] < 2000:
        return {
            "message": f"La película '{movie_data['title']}' tuvo menos de 2000 valoraciones."
        }
    else:
        return {
            "message": f"La película '{movie_data['title']}' tuvo {int(movie_data['vote_count'])} votos y su puntaje promedio fue {float(movie_data['vote_average']):.2f}."
        }

@app.get("/score/")
async def get_movie_score(title: str):
    movie = scores_df[scores_df['title'].str.lower() == title.lower()]
    if movie.empty:
        raise HTTPException(status_code=404, detail="Película no encontrada.")
    
    movie_data = movie.iloc[0]
    return {
        "message": f"La película '{movie_data['title']}' fue estrenada en el año {int(movie_data['release_year'])}, con una popularidad de {float(movie_data['popularity']):.2f}."
    }

@app.get("/titles/")
async def get_titles():
    return votes_df['title'].tolist()  # Asumiendo que ambos DataFrames tienen los mismos títulos

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
