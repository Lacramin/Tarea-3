import time
import os
import random
import json
import uuid
import numpy as np
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
DISTRIBUCION = os.getenv("DISTRIBUCION", "zipf").lower()
TOTAL_CONSULTAS = int(os.getenv("TOTAL_CONSULTAS", "6000"))
SLEEP_ENTRE_CONSULTAS = float(os.getenv("SLEEP_ENTRE_CONSULTAS", "0.01"))
CONFIDENCE_VARIATION = os.getenv("CONFIDENCE_VARIATION", "random").lower()
CONFIDENCE_FIXED = float(os.getenv("CONFIDENCE_FIXED", "0.0"))

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]
TOPIC_PRINCIPAL = "consultas_geo"

if DISTRIBUCION == "zipf":
    weights = [1/(i**1.5) for i in range(1, len(ZONAS) + 1)]
    weights = np.array(weights) / np.sum(weights)
    print(f"Distribucion Zipf - Pesos: {dict(zip(ZONAS, weights.round(3)))}", flush=True)
else:
    weights = None
    print("Distribucion Uniforme configurada", flush=True)

def esperar_kafka():
    print(f"Conectando a Kafka en {KAFKA_BROKER}...", flush=True)
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Conectado a Kafka exitosamente.", flush=True)
            return producer
        except Exception as e:
            print("Kafka no esta listo, reintentando en 3s...", flush=True)
            time.sleep(3)

def seleccionar_zona():
    if DISTRIBUCION == "zipf":
        return np.random.choice(ZONAS, p=weights)
    else:
        return random.choice(ZONAS)

def generar_parametros():
    if CONFIDENCE_VARIATION == "fixed":
        conf = CONFIDENCE_FIXED
    else:
        conf = round(random.uniform(0.0, 0.99), 2)
    bins = random.choice([2, 3, 5, 7, 10, 12, 15, 20, 25, 30])
    return conf, bins

if __name__ == "__main__":
    producer = esperar_kafka()
    
    print(f"Iniciando envio de {TOTAL_CONSULTAS} consultas a Kafka...")
    print(f"Distribucion: {DISTRIBUCION.upper()}")
    print("-" * 60, flush=True)

    tiempo_inicio = time.time()
    
    for i in range(1, TOTAL_CONSULTAS + 1):
        q = random.choice(CONSULTAS)
        z = seleccionar_zona()
        conf, bins = generar_parametros()
        
        # Estructura del mensaje segun los requerimientos de la Tarea 2
        payload = {
            "id_consulta": str(uuid.uuid4()),
            "query": q,
            "zona_a": z,
            "parametros": {
                "confidence_min": conf
            },
            "intentos": 0,
            "timestamp_creacion": time.time()
        }
        
        if q == "q4":
            z_b = random.choice([zona for zona in ZONAS if zona != z])
            payload["parametros"]["zona_b"] = z_b
        elif q == "q5":
            payload["parametros"]["bins"] = bins
            
        # Enviar mensaje a Kafka
        producer.send(TOPIC_PRINCIPAL, value=payload)
        
        if i % 100 == 0 or i == 1:
            print(f"[{i}/{TOTAL_CONSULTAS}] Mensaje enviado a Kafka -> {q.upper()} en {z}", flush=True)
            
        time.sleep(SLEEP_ENTRE_CONSULTAS)
        
    producer.flush()
    tiempo_total = time.time() - tiempo_inicio
    
    print("\n" + "=" * 60)
    print("SIMULACION TERMINADA")
    print("=" * 60)
    print(f"Total mensajes enviados a Kafka: {TOTAL_CONSULTAS}")
    print(f"Tiempo total: {tiempo_total:.2f}s")
    print(f"Throughput de envio: {TOTAL_CONSULTAS/tiempo_total:.2f} msg/s")
    print("=" * 60, flush=True)
    
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nGenerador detenido manualmente.", flush=True)
