import os
import json
import pandas as pd
import psycopg2
import shutil
import logging
from datetime import datetime

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# Directorios
DATA_DIR = "/opt/airflow/data/pendientes"
CONFIG_DIR = "/opt/airflow/data/config"
PROCESADO_DIR = "/opt/airflow/data/procesado"
ERROR_DIR = "/opt/airflow/data/error"
LOG_FILE = "/opt/airflow/logs/etl_multi_log.csv"

# DB config
DB_CONFIG = {
    'host': 'etl-postgres.cr8mci2w46y9.us-west-2.rds.amazonaws.com',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Postgres123!',
    'port': '5432'
}

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def escribir_log(nombre_archivo, estado, mensaje):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()},{nombre_archivo},{estado},{mensaje}\n")

def cargar_config(nombre_archivo):
    config_path = os.path.join(CONFIG_DIR, f"{nombre_archivo}.json")
    with open(config_path) as f:
        return json.load(f)

def validar_fila(row, config):
    errores = []

    # Validar campos no nulos
    for campo in config.get("validar_no_nulos", []):
        if pd.isnull(row[campo]) or str(row[campo]).strip() == "":
            errores.append(f"Campo '{campo}' es nulo o vacío")

    # Validar tipos de datos y rangos
    tipos = config.get("tipos", {})
    for campo, tipo_esperado in tipos.items():
        valor = row.get(campo)

        if pd.isnull(valor) or str(valor).strip() == "":
            # Si es nulo o vacío y el campo es obligatorio, ya fue capturado arriba
            # Si no es obligatorio, dejamos pasar el nulo
            continue

        try:
            if tipo_esperado == "int":
                v = int(float(valor))
                if not (-9223372036854775808 <= v <= 9223372036854775807):
                    errores.append(f"Campo '{campo}' fuera de rango BIGINT: {v}")
            elif tipo_esperado == "float":
                float(valor)
            elif tipo_esperado == "str":
                str(valor)
        except Exception as e:
            errores.append(f"Campo '{campo}' no es del tipo {tipo_esperado}: {e}")

    return errores

def procesar_csv(file_path):
    try:
        nombre_archivo = os.path.basename(file_path)
        config = cargar_config(nombre_archivo.replace(".csv", ""))
        df = pd.read_csv(file_path)

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insertados = 0
        rechazados = 0

        for _, row in df.iterrows():
            errores = validar_fila(row, config)
            if errores:
                # Insertar en tabla de rechazados
                cursor.execute(
                    """
                    INSERT INTO users_rechazados (id, name, age, email, motivo_rechazo)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (row.get('id'), row.get('name'), row.get('age'), row.get('email'), "; ".join(errores))
                )
                rechazados += 1
            else:
                try:
                    id_val = int(float(row['id']))
                    age_val = int(float(row['age'])) if pd.notnull(row['age']) and str(row['age']).strip() != "" else None
                    email_val = row['email'] if pd.notnull(row['email']) and str(row['email']).strip() != "" else None

                    cursor.execute(
                        f"""
                        INSERT INTO {config['tabla_destino']} (id, name, age, email)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (id_val, row['name'], age_val, email_val)
                    )
                    if cursor.rowcount == 1:
                        insertados += 1
                except Exception as e:
                    log.error(f"❌ Error insertando fila: {row} - {e}")
                    raise e

        conn.commit()
        cursor.close()
        conn.close()

        mensaje = f"{insertados} insertados, {rechazados} rechazados"
        log.info(f"✅ Inserción completada: {mensaje}")
        return True, mensaje

    except Exception as e:
        log.error(f"❌ Error procesando {file_path}: {e}")
        return False, str(e)

def main():
    archivos = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    if not archivos:
        log.warning("⚠️ No hay archivos CSV en carpeta pendientes")
        return

    hubo_error = False

    for archivo in archivos:
        full_path = os.path.join(DATA_DIR, archivo)
        exito, mensaje = procesar_csv(full_path)

        destino = PROCESADO_DIR if exito else ERROR_DIR
        shutil.move(full_path, os.path.join(destino, archivo))
        estado = "OK" if exito else "ERROR"
        escribir_log(archivo, estado, mensaje)

        log.info(f"{'✔️ Procesado' if exito else '❌ Error'}: {archivo} -> {destino} - {mensaje}")
        print(f"{'✅' if exito else '❌'} {archivo}: {mensaje}")

        if not exito:
            hubo_error = True

    if hubo_error:
        log.error("🛑 Terminando script con error debido a uno o más archivos fallidos")
        exit(1)

    log.info("✅ Todos los archivos fueron procesados exitosamente")

if __name__ == "__main__":
    log.info("🚀 Script etl_multi_config.py iniciado")
    main()

