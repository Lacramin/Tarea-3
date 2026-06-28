#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} INICIANDO BATERIA DE PRUEBAS - TAREA 2 ${NC}"
echo -e "${GREEN}========================================${NC}"

# Parámetros fijos para el caché según Tarea 2
CACHE_MEMORY="50mb"
CACHE_POLICY="allkeys-lru"

run_escenario() {
    local ESCENARIO=$1
    local DIST=$2
    local CONSUMIDORES=$3
    local SIMULAR_FALLA=$4
    
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW} EJECUTANDO: ${ESCENARIO}${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    echo "Deteniendo servicios anteriores..."
    docker compose -f docker-compose-temp.yml down -v 2>/dev/null
    docker compose down -v 2>/dev/null
    
    echo "Configurando entorno temporal..."
    
    cat > docker-compose-temp.yml << EOF
services:
  kafka:
    image: apache/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    healthcheck:
      test: ["CMD", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis-cache:
    image: redis:alpine
    ports:
      - "6379:6379"
    command: redis-server --maxmemory ${CACHE_MEMORY} --maxmemory-policy ${CACHE_POLICY}
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  metricas:
    build: ./metricas
    ports:
      - "8001:8000"
    volumes:
      - ./metricas:/app

  generador_respuestas:
    build: ./generador_respuestas
    ports:
      - "8002:8000"
    environment:
      - SIMULAR_FALLA=${SIMULAR_FALLA}

  cache_system:
    build: ./cache_system
    ports:
      - "8003:8000"
    depends_on:
      redis-cache:
        condition: service_healthy
      generador_respuestas:
        condition: service_started
      metricas:
        condition: service_started
    environment:
      - REDIS_HOST=redis-cache
      - RESPUESTAS_URL=http://generador_respuestas:8000
      - METRICAS_URL=http://metricas:8000

  generador_trafico:
    build: ./generador_trafico
    depends_on:
      - kafka
    environment:
      - KAFKA_BROKER=kafka:9092
      - DISTRIBUCION=${DIST}
      - TOTAL_CONSULTAS=6000
      - SLEEP_ENTRE_CONSULTAS=0.01
      - CONFIDENCE_VARIATION=random

  consumidor_kafka:
    build: ./consumidor_kafka
    depends_on:
      - kafka
      - cache_system
    environment:
      - KAFKA_BROKER=kafka:9092
      - CACHE_URL=http://cache_system:8000
EOF

    echo "Levantando infraestructura base (Kafka, Redis, Backend, Metricas)..."
    docker compose -f docker-compose-temp.yml up -d redis-cache metricas generador_respuestas cache_system kafka
    
    echo "Esperando inicializacion de servicios..."
    sleep 15
    
    echo "Levantando consumidores (Replicas: ${CONSUMIDORES})..."
    docker compose -f docker-compose-temp.yml up -d --scale consumidor_kafka=${CONSUMIDORES} consumidor_kafka
    sleep 5
    
    echo "Reseteando metricas..."
    python3 -c "import requests; requests.get('http://localhost:8001/reset')" 2>/dev/null || true
    
    echo "Iniciando generador de trafico..."
    docker compose -f docker-compose-temp.yml up generador_trafico &
    TRAFFIC_PID=$!
    
    echo "Procesando consultas (~90 segundos)..."
    sleep 90
    
    echo "Deteniendo generador y contenedores..."
    docker compose -f docker-compose-temp.yml stop generador_trafico 2>/dev/null
    kill $TRAFFIC_PID 2>/dev/null
    sleep 3
    
    echo "Descargando registro de metricas..."
    mkdir -p metricas
    python3 -c "
import requests
r = requests.get('http://localhost:8001/descargar')
if r.status_code == 200:
    with open('metricas/${ESCENARIO}.csv', 'wb') as f:
        f.write(r.content)
    print('Descarga completa.')
else:
    print('Error en descarga:', r.status_code)
"
    
    if [ -f "metricas/${ESCENARIO}.csv" ] && [ -s "metricas/${ESCENARIO}.csv" ]; then
        LINEAS=$(wc -l < "metricas/${ESCENARIO}.csv")
        echo -e "${GREEN}Archivo guardado: metricas/${ESCENARIO}.csv (${LINEAS} lineas)${NC}"
    else
        echo -e "${RED}Error: El archivo CSV no se genero correctamente.${NC}"
    fi
    
    echo "Estadisticas de la prueba:"
    python3 -c "import requests; print(requests.get('http://localhost:8001/stats').json())" 2>/dev/null || echo "Estadisticas no disponibles"
    
    echo "Limpiando entorno..."
    docker compose -f docker-compose-temp.yml down -v 2>/dev/null
    rm -f docker-compose-temp.yml
    sleep 5
}

run_escenario "kafka_1_consumer_zipf" "zipf" 1 "false"
run_escenario "kafka_3_consumers_zipf" "zipf" 3 "false"
run_escenario "kafka_1_consumer_uniform" "uniform" 1 "false"
run_escenario "falla_temporal_zipf" "zipf" 1 "true"
run_escenario "spike_trafico_3_consumers" "zipf" 3 "false"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN} PRUEBAS COMPLETADAS ${NC}"
echo -e "${GREEN}========================================${NC}"
ls -lh metricas/*.csv 2>/dev/null
