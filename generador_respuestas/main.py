from fastapi import FastAPI, Query
import pandas as pd
import numpy as np
import os
import time
import random

app = FastAPI()

# Leemos la variable de entorno para saber si debemos fallar a propósito
SIMULAR_FALLA = os.getenv("SIMULAR_FALLA", "false").lower() == "true"

# ============================================
# CONFIGURACIÓN DE ZONAS (BOUNDING BOXES REALES)
# ============================================
ZONAS_BBOX = {
    "Z1": {"lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600, "name": "Providencia"},
    "Z2": {"lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550, "name": "Las Condes"},
    "Z3": {"lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740, "name": "Maipú"},
    "Z4": {"lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630, "name": "Santiago Centro"},
    "Z5": {"lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760, "name": "Pudahuel"},
}

# ============================================
# 1. PRECARGA DE DATOS POR ZONA
# ============================================
data_por_zona = {}
zone_area_km2 = {}

print(" Cargando dataset completo...", flush=True)
FILE_PATH = "967_buildings.csv.gz"

if not os.path.exists(FILE_PATH):
    print(f" error: No encontramos :(( {FILE_PATH}", flush=True)
    df = pd.DataFrame()
else:
    df = pd.read_csv(FILE_PATH, compression='gzip')
    print(f" Dataset cargado :)) : {len(df)} edificios", flush=True)

    for zona_id, bbox in ZONAS_BBOX.items():
        mask = (
            (df['latitude'] >= bbox['lat_min']) & (df['latitude'] <= bbox['lat_max']) &
            (df['longitude'] >= bbox['lon_min']) & (df['longitude'] <= bbox['lon_max'])
        )
        data_por_zona[zona_id] = df[mask].copy()
        
        # Calcular área en km²
        lat_diff = bbox['lat_max'] - bbox['lat_min']
        lon_diff = bbox['lon_max'] - bbox['lon_min']
        km_per_deg_lat = 111.32
        km_per_deg_lon = 111.32 * np.cos(np.mean([bbox['lat_min'], bbox['lat_max']]) * np.pi / 180)
        zone_area_km2[zona_id] = abs(lat_diff * km_per_deg_lat * lon_diff * abs(km_per_deg_lon))
        
        print(f"   {zona_id} ({bbox['name']}): {len(data_por_zona[zona_id])} edificios, {zone_area_km2[zona_id]:.2f} km²", flush=True)

print("Todas las zonas cargadas en memoria.", flush=True)

# ============================================
# 2. ENDPOINT ÚNICO - RUTA /data/{zona}/{query}
# ============================================

@app.get("/data/{zona_id}/{query}")
def procesar_consulta(
    zona_id: str, 
    query: str, 
    confidence_min: float = Query(0.0),
    bins: int = Query(5),
    zona_b: str = Query(None)
):
    """Endpoint genérico que redirige según tipo de consulta"""
    
    # Inyección de fallos artificiales
    if SIMULAR_FALLA:
        if random.random() < 0.40:
            return {"error": "Falla simulada del backend para probar Kafka"}

    if zona_id not in data_por_zona:
        return {"error": "Zona no válida"}
    
    if query == "q1":
        return q1_count(zona_id, confidence_min)
    elif query == "q2":
        return q2_area(zona_id, confidence_min)
    elif query == "q3":
        return q3_density(zona_id, confidence_min)
    elif query == "q4":
        if zona_b is None:
            return {"error": "zona_b es requerida para q4"}
        return q4_compare(zona_id, zona_b, confidence_min)
    elif query == "q5":
        return q5_confidence_dist(zona_id, bins)
    else:
        return {"error": "Consulta no válida"}

def q1_count(zona_id: str, confidence_min: float):
    start = time.time()
    records = data_por_zona[zona_id]
    count = int((records['confidence'] >= confidence_min).sum())
    elapsed = time.time() - start
    
    return {
        "zona": zona_id,
        "consulta": "q1",
        "confidence_min": confidence_min,
        "resultado": count,
        "tiempo_procesamiento_ms": round(elapsed * 1000, 2)
    }

def q2_area(zona_id: str, confidence_min: float):
    start = time.time()
    records = data_por_zona[zona_id]
    filtradas = records[records['confidence'] >= confidence_min]
    
    if len(filtradas) == 0:
        return {"zona": zona_id, "consulta": "q2", "avg_area": 0, "total_area": 0, "n": 0}
    
    avg_area = float(filtradas['area_in_meters'].mean())
    total_area = float(filtradas['area_in_meters'].sum())
    elapsed = time.time() - start
    
    return {
        "zona": zona_id,
        "consulta": "q2",
        "confidence_min": confidence_min,
        "avg_area": round(avg_area, 2),
        "total_area": round(total_area, 2),
        "n": len(filtradas),
        "tiempo_procesamiento_ms": round(elapsed * 1000, 2)
    }

def q3_density(zona_id: str, confidence_min: float):
    start = time.time()
    records = data_por_zona[zona_id]
    count = int((records['confidence'] >= confidence_min).sum())
    density = count / zone_area_km2[zona_id] if zone_area_km2[zona_id] > 0 else 0
    elapsed = time.time() - start
    
    return {
        "zona": zona_id,
        "consulta": "q3",
        "confidence_min": confidence_min,
        "densidad_por_km2": round(density, 2),
        "area_km2": round(zone_area_km2[zona_id], 2),
        "n_edificios": count,
        "tiempo_procesamiento_ms": round(elapsed * 1000, 2)
    }

def q4_compare(zona_a: str, zona_b: str, confidence_min: float):
    if zona_a not in data_por_zona or zona_b not in data_por_zona:
        return {"error": "Zona no válida"}
    
    start = time.time()
    da = int((data_por_zona[zona_a]['confidence'] >= confidence_min).sum()) / zone_area_km2[zona_a] if zone_area_km2[zona_a] > 0 else 0
    db = int((data_por_zona[zona_b]['confidence'] >= confidence_min).sum()) / zone_area_km2[zona_b] if zone_area_km2[zona_b] > 0 else 0
    elapsed = time.time() - start
    
    return {
        "consulta": "q4",
        "confidence_min": confidence_min,
        "zone_a": {"id": zona_a, "densidad": round(da, 2)},
        "zone_b": {"id": zona_b, "densidad": round(db, 2)},
        "winner": zona_a if da > db else zona_b,
        "tiempo_procesamiento_ms": round(elapsed * 1000, 2)
    }

def q5_confidence_dist(zona_id: str, bins: int):
    start = time.time()
    scores = data_por_zona[zona_id]['confidence'].values
    
    if len(scores) == 0:
        return {"zona": zona_id, "consulta": "q5", "distribucion": []}
    
    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
    distribucion = [
        {"bucket": i, "min": round(float(edges[i]), 2), 
         "max": round(float(edges[i+1]), 2), "count": int(counts[i])}
        for i in range(bins)
    ]
    elapsed = time.time() - start
    
    return {
        "zona": zona_id,
        "consulta": "q5",
        "bins": bins,
        "distribucion": distribucion,
        "tiempo_procesamiento_ms": round(elapsed * 1000, 2)
    }

@app.get("/health")
def health():
    return {"status": "ok", "zonas_cargadas": list(data_por_zona.keys())}
