from fastapi import FastAPI, Query
import redis, os, requests, json, time
from datetime import datetime

aplicacion = FastAPI()

servidor_redis = os.getenv("REDIS_HOST", "redis-cache")
url_respuestas = os.getenv("RESPUESTAS_URL", "http://generador_respuestas:8000")
url_metricas = os.getenv("METRICAS_URL", "http://metricas:8000")

cliente_redis = redis.Redis(host=servidor_redis, port=6379, db=0, decode_responses=True)

def enviar_registro(tipo_consulta, latencia, resultado_cache, reintentos, estado_final):
    datos_metrica = {
        "marca_tiempo": datetime.now().isoformat(),
        "tipo_consulta": tipo_consulta,
        "latencia": latencia,
        "resultado_cache": resultado_cache,
        "cantidad_reintentos": reintentos,
        "estado_final": estado_final
    }
    try:
        requests.post(f"{url_metricas}/registrar", json=datos_metrica, timeout=0.5)
    except:
        pass

@aplicacion.get("/consulta/{consulta}/{zona_a}")
def consultar_simple(
    consulta: str, 
    zona_a: str, 
    confidence_min: float = Query(0.0), 
    bins: int = Query(5),
    intentos: int = Query(0)
):
    tiempo_inicio = time.time()
    
    if consulta == "q5":
        llave = f"distribucion_confianza:{zona_a}:tramos={bins}"
    elif consulta == "q1":
        llave = f"conteo:{zona_a}:conf={confidence_min}"
    elif consulta == "q2":
        llave = f"area:{zona_a}:conf={confidence_min}"
    elif consulta == "q3":
        llave = f"densidad:{zona_a}:conf={confidence_min}"
    else:
        llave = f"{consulta}:{zona_a}:conf={confidence_min}"
    
    resultado_cacheado = cliente_redis.get(llave)
    if resultado_cacheado:
        latencia = round(time.time() - tiempo_inicio, 6)
        enviar_registro(consulta, latencia, "hit", intentos, "exito")
        return json.loads(resultado_cacheado)
    
    try:
        url_servicio = f"{url_respuestas}/data/{zona_a}/{consulta}"
        parametros = {"confidence_min": confidence_min, "bins": bins}
        
        respuesta = requests.get(url_servicio, params=parametros, timeout=5)
        respuesta.raise_for_status()
        datos = respuesta.json()
        
        cliente_redis.setex(llave, 60, json.dumps(datos))
        latencia = round(time.time() - tiempo_inicio, 6)
        enviar_registro(consulta, latencia, "miss", intentos, "exito")
        return datos

    except Exception as e:
        latencia = round(time.time() - tiempo_inicio, 6)
        enviar_registro(consulta, latencia, "miss", intentos, "error")
        return {"error": "backend_inaccesible", "detalle": str(e)}

@aplicacion.get("/consulta/{consulta}/{zona_a}/{zona_b}")
def consultar_doble(
    consulta: str, 
    zona_a: str, 
    zona_b: str, 
    confidence_min: float = Query(0.0),
    intentos: int = Query(0)
):
    tiempo_inicio = time.time()
    
    llave = f"comparar:densidad:{zona_a}:{zona_b}:conf={confidence_min}"
    
    resultado_cacheado = cliente_redis.get(llave)
    if resultado_cacheado:
        latencia = round(time.time() - tiempo_inicio, 6)
        enviar_registro(consulta, latencia, "hit", intentos, "exito")
        return json.loads(resultado_cacheado)
    
    try:
        url_servicio = f"{url_respuestas}/data/{zona_a}/{consulta}"
        parametros = {"confidence_min": confidence_min, "zona_b": zona_b}
        
        respuesta = requests.get(url_servicio, params=parametros, timeout=5)
        datos = respuesta.json()
        
        cliente_redis.setex(llave, 60, json.dumps(datos))
        latencia = round(time.time() - tiempo_inicio, 6)
        enviar_registro(consulta, latencia, "miss", intentos, "exito")
        return datos
    except Exception as e:
        latencia = round(time.time() - tiempo_inicio, 6)
        enviar_registro(consulta, latencia, "miss", intentos, "error")
        return {"error": "caido", "detalle": str(e)}

@aplicacion.get("/health")
def estado_salud():
    try:
        cliente_redis.ping()
        return {"estado": "ok", "redis": "conectado"}
    except:
        return {"estado": "degradado", "redis": "desconectado"}
