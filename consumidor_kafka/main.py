import os
import json
import time
import requests
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

servidor_kafka = os.getenv("KAFKA_BROKER", "kafka:9092")
url_cache = os.getenv("CACHE_URL", "http://cache_system:8000")
url_metricas = "http://metricas:8000"

topico_principal = "consultas_geo"
topico_reintento = "consultas_reintento"
topico_dlq = "consultas_dlq"

grupo_consumo = "grupo-consumidores-1"
maximo_intentos = 3

def iniciar_conexion_kafka():
    while True:
        try:
            consumidor = KafkaConsumer(
                topico_principal,
                topico_reintento,
                bootstrap_servers=[servidor_kafka],
                value_deserializer=lambda mensaje: json.loads(mensaje.decode('utf-8')),
                group_id=grupo_consumo,
                auto_offset_reset='earliest'
            )
            
            productor = KafkaProducer(
                bootstrap_servers=[servidor_kafka],
                value_serializer=lambda valor: json.dumps(valor).encode('utf-8')
            )
            return consumidor, productor
        except Exception:
            time.sleep(3)

def enviar_registro_fallo(tipo_consulta, resultado_cache, reintentos, estado_final):
    datos_metrica = {
        "marca_tiempo": datetime.now().isoformat(),
        "tipo_consulta": tipo_consulta,
        "latencia": 0.0,
        "resultado_cache": resultado_cache,
        "cantidad_reintentos": reintentos,
        "estado_final": estado_final
    }
    try:
        requests.post(f"{url_metricas}/registrar", json=datos_metrica, timeout=1)
    except:
        pass

if __name__ == "__main__":
    consumidor, productor = iniciar_conexion_kafka()
    
    for mensaje in consumidor:
        carga_util = mensaje.value
        consulta = carga_util.get("query")
        zona_a = carga_util.get("zona_a")
        parametros = carga_util.get("parametros", {})
        intentos = carga_util.get("intentos", 0)
        
        if consulta == "q4":
            zona_b = parametros.get("zona_b")
            url_peticion = f"{url_cache}/consulta/{consulta}/{zona_a}/{zona_b}"
        else:
            url_peticion = f"{url_cache}/consulta/{consulta}/{zona_a}"
            
        parametros_peticion = {clave: valor for clave, valor in parametros.items() if clave != "zona_b"}
        parametros_peticion["intentos"] = intentos
        
        try:
            respuesta = requests.get(url_peticion, params=parametros_peticion, timeout=5)
            datos = respuesta.json()
            
            if respuesta.status_code != 200 or "error" in datos:
                raise Exception("Fallo en el servicio")
                
        except Exception as e:
            carga_util["intentos"] += 1
            
            if carga_util["intentos"] < maximo_intentos:
                productor.send(topico_reintento, value=carga_util)
                enviar_registro_fallo(consulta, "miss", carga_util["intentos"], "reintento")
            else:
                productor.send(topico_dlq, value=carga_util)
                enviar_registro_fallo(consulta, "miss", carga_util["intentos"], "dlq")
                
    productor.flush()
