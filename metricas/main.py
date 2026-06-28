from fastapi import FastAPI, Request
from kafka import KafkaProducer
import json
import os

app = FastAPI()

servidor_kafka = os.getenv("KAFKA_BROKER", "kafka:9092")

productor = KafkaProducer(
    bootstrap_servers=[servidor_kafka],
    value_serializer=lambda valor: json.dumps(valor).encode('utf-8')
)

@app.post("/registrar")
async def registrar(peticion: Request):
    datos = await peticion.json()
    productor.send("metrics-topic", value=datos)
    productor.flush()
    return {"estado": "enviado"}
