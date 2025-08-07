import os
import json
import pandas as pd
import psycopg2
import shutil
import logging
from datetime import datetime

# Configuración
DATA_DIR = "/opt/airflow/data/pendientes"
CONFIG_DIR = "/opt/airflow/data/config"
PROCESADO_DIR = "/opt/airflow/data/procesado"
ERROR_DIR = "/opt/airflow/data/error"
LOG_FILE = "/opt/airflow/logs/etl_multi_log.csv"

DB_CONFIG = {
    'host': 'etl-postgres.cr8mci2w46y9.us-west-2.rds.amazonaws.com',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Postgres123!',
    'port': '5432'
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def escribir_log(nombre_archivo, estado, insertados, omitidos, rechazados, mensaje):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()},{nombre_archivo},{estado},{insertados},{omitidos},{rechazados},\"{mensaje}\"\n")

def validar_row(row, reglas):
    errores = []

    for campo, regla in reglas.items():
        valor = row.get(campo)

        if regla == "not_null":
            if pd.isna(valor) or str(valor).strip() == "":
                errores.append(f"{campo} no puede ser nulo o vacío")
        elif regla == "positive_integer":
            try:
                if int(valor) <= 0:
                    errores.append(f"{campo} debe ser positivo")
            except:
                errores.append(f"{campo} no es un número entero válido")

    return errores

def procesar_csv_con_config(file_path, nombre_archivo):
    insertados = 0
    omitidos = 0
    rechazados = 0
    errores_archivo = []

    try:
        config_path = os.path.join(CONFIG_DIR, f"{nombre_archivo}.json")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"No se encontró archivo de configuración para {nombre_archivo}")

        with open(config_path) as f:
            config = json.load(f)

        tabla_destino = config["tabla_destino"]
        mapeo = config["mapeo_columnas"]
        reglas_validacion = config.get("validaciones", {})

        df = pd.read_csv(file_path)
        df.rename(columns=mapeo, inplace=True)

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            errores = validar_row(row, reglas_validacion)

            if errores:
                rechazados += 1
                try:
                    conn.rollback()
                    cursor.execute(
                        f"""
                        INSERT INTO users_rechazados (id, name, age, email, error_mensaje, archivo)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (row.get('id'), row.get('name'), row.get('age'), row.get('email'),
                         "; ".join(errores), nombre_archivo)
                    )
                    conn.commit()
                except Exception as e2:
                    log.warning(f"⚠️ No se pudo insertar en users_rechazados: {e2}")
                continue

            try:
                cursor.execute(
                    f"""
                    INSERT INTO {tabla_destino} (id, name, age, email)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (row['id'], row['name'], row['age'], row['email'])
                )
                if cursor.rowcount == 1:
                    insertados += 1
                else:
                    omitidos += 1
            except Exception as e:
                rechazados += 1
                errores_archivo.append(str(e))
                try:
                    conn.rollback()
                    cursor.execute(
                        f"""
                        INSERT INTO users_rechazados (id, name, age, email, error_mensaje, archivo)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (row.get('id'), row.get('name'), row.get('age'), row.get('email'), str(e), nombre_archivo)
                    )
                    conn.commit()
                except Exception as e2:
                    log.warning(f"⚠️ No se pudo insertar en users_rechazados: {e2}")

        conn.commit()
        cursor.close()
        conn.close()

        mensaje = f"{insertados} insertados, {omitidos} omitidos, {rechazados} rechazados"
        return (rechazados == 0), insertados, omitidos, rechazados, mensaje

    except Exception as e:
        log.error(f"❌ Error general al procesar {file_path}: {e}")
        return False, 0, 0, 0, str(e)

def main():
    log.info("🚀 Iniciando procesamiento dinámico con configuración")
    archivos = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    if not archivos:
        log.warning("⚠️ No se encontraron archivos CSV pendientes")
        return

    for archivo in archivos:
        full_path = os.path.join(DATA_DIR, archivo)
        exito, ins, omi, rech, mensaje = procesar_csv_con_config(full_path, archivo)
        destino = PROCESADO_DIR if exito else ERROR_DIR
        shutil.move(full_path, os.path.join(destino, archivo))
        estado = "OK" if exito else "ERROR"
        escribir_log(archivo, estado, ins, omi, rech, mensaje)
        log.info(f"{'✔️ Procesado' if exito else '❌ Con errores'}: {archivo} -> {destino} - {mensaje}")

if __name__ == "__main__":
    main()