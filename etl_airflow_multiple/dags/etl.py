import pandas as pd
import psycopg2
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_etl():
    logging.info("¡ETL ejecutándose bro desde el script externo!")
    print("print también funciona desde run_etl")

    try:
        logging.info("Inicio del proceso ETL.")

        # Leer archivo CSV
        logging.info("Leyendo el archivo CSV...")
        df = pd.read_csv('/opt/airflow/data/sample_data.csv')
        logging.info(f"{len(df)} registros leídos del archivo.")

        # Validar columna 'email' y eliminar nulos
        if 'email' in df.columns:
            logging.info("Validando columna 'email' y eliminando valores nulos...")
            df = df.dropna(subset=['email'])
        else:
            logging.warning("La columna 'email' no existe en el DataFrame.")
            return

        # Conectar a PostgreSQL
        logging.info("Conectando a la base de datos PostgreSQL...")
        conn = psycopg2.connect(
            host='etl-postgres.cr8mci2w46y9.us-west-2.rds.amazonaws.com',
            database='postgres',
            user='postgres',
            password='Postgres123!',
            port='5432'
        )
        cursor = conn.cursor()
        logging.info("Conexión establecida con éxito.")

        # Crear tabla
        logging.info("Creando tabla 'users' en PostgreSQL...")
        cursor.execute('DROP TABLE IF EXISTS users;')
        cursor.execute('''
            CREATE TABLE users (
                id INT,
                name TEXT,
                age INT,
                email TEXT
            );
        ''')
        logging.info("Tabla 'users' creada correctamente.")

        # Insertar datos
        logging.info("Insertando datos en la tabla...")
        for i, row in df.iterrows():
            cursor.execute(
                "INSERT INTO users (id, name, age, email) VALUES (%s, %s, %s, %s)",
                (int(row['id']), row['name'], int(row['age']), row['email'])
            )

        # Confirmar y cerrar
        conn.commit()
        conn.close()
        logging.info("Proceso ETL finalizado correctamente.")

    except Exception as e:
        logging.error(f"Ocurrió un error durante el ETL: {e}")
        raise