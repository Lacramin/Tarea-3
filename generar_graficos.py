import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
from glob import glob

# Crear carpeta para los gráficos de la Tarea 2
os.makedirs("metricas/graficos_tarea2", exist_ok=True)

archivos = sorted(glob("metricas/*.csv"))
resultados = []

for archivo in archivos:
    nombre = os.path.basename(archivo).replace(".csv", "")
    if 'registro_metricas' in nombre:
        continue
        
    try:
        df = pd.read_csv(archivo)
        df.columns = df.columns.str.strip()
        df['evento'] = df['evento'].astype(str).str.strip().str.upper()
        
        # Conteo de los eventos de la Tarea 2
        hits = len(df[df['evento'] == 'HIT'])
        misses = len(df[df['evento'] == 'MISS'])
        retries = len(df[df['evento'] == 'RETRY'])
        dlqs = len(df[df['evento'] == 'DLQ'])
        
        total_procesadas = hits + misses
        total_mensajes_unicos = total_procesadas + dlqs
        
        if total_mensajes_unicos > 0:
            # Cálculos de métricas exigidas
            retry_rate = (retries / (total_mensajes_unicos + retries)) * 100
            dlq_rate = (dlqs / total_mensajes_unicos) * 100
            
            resultados.append({
                "Escenario": nombre,
                "Hits": hits,
                "Misses": misses,
                "Retries": retries,
                "DLQs": dlqs,
                "Retry Rate (%)": round(retry_rate, 2),
                "DLQ Rate (%)": round(dlq_rate, 2)
            })
            
            # Gráfico Circular (Pie Chart) 
            plt.figure(figsize=(8, 8))
            etiquetas = ['Éxito (Caché)', 'Éxito (Backend)', 'Reintentos', 'Perdidos (DLQ)']
            valores = [hits, misses, retries, dlqs]
            colores = ['#2ecc71', '#3498db', '#f1c40f', '#e74c3c']
            
            # Limpiar rebanadas con valor cero para que el gráfico quede limpio
            etiquetas_filtradas = [e for i, e in enumerate(etiquetas) if valores[i] > 0]
            valores_filtrados = [v for v in valores if v > 0]
            colores_filtrados = [c for i, c in enumerate(colores) if valores[i] > 0]
            
            plt.pie(valores_filtrados, labels=etiquetas_filtradas, colors=colores_filtrados, 
                    autopct='%1.1f%%', startangle=140, shadow=True, 
                    textprops={'fontsize': 11, 'weight': 'bold'})
            
            plt.title(f'Distribución del Flujo con Kafka\n({nombre})', fontsize=14, pad=20)
            plt.axis('equal')
            
            plt.tight_layout()
            plt.savefig(f'metricas/graficos_tarea2/torta_{nombre}.png', dpi=300)
            plt.close()
            
    except Exception as e:
        print(f"Error al procesar el archivo {archivo}: {e}")

# Tabla de resumen
df_resumo = pd.DataFrame(resultados)
if not df_resumo.empty:
    print("\n=== RESUMEN MÉTRICAS DE TOLERANCIA A FALLOS ===")
    print(df_resumo.to_string(index=False))
    df_resumo.to_csv("metricas/graficos_tarea2/resumen_tarea2.csv", index=False)
    print("\n Los gráficos y el CSV de resumen fueron guardados en: metricas/graficos_tarea2/")
else:
    print(" No se encontraron datos válidos.")
