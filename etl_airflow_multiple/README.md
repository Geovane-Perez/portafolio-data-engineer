🛠️ Proyecto ETL Dinámico con Airflow y PostgreSQL

Fecha de implementación: Agosto 2025 
Autor: Geovane Pérez
Repositorio: portafolio-data-engineer

🎯 Objetivo del Proyecto

Construir un pipeline ETL flexible y escalable usando Apache Airflow, capaz de procesar múltiples archivos .csv con base en archivos de configuración .json, validando y cargando los datos en una base de datos PostgreSQL alojada en AWS RDS.

⚙️ Componentes Clave
	•	Airflow DAG (etl_multi_config_dag.py): Ejecuta el pipeline usando un BashOperator.
	•	Script ETL (etl_multi_config.py):
	•	Lee todos los archivos CSV en la carpeta /data/pendientes.
	•	Busca un archivo .json con la configuración correspondiente en /data/config/.
	•	Valida los datos (campos nulos, tipos, rangos).
	•	Inserta datos válidos en la tabla configurada (ej. users).
	•	Registra datos inválidos en la tabla users_rechazados.
	•	Mueve el archivo a /data/procesado/ o /data/error/ según resultado.
	•	Registra el estado de cada archivo en un log CSV (etl_multi_log.csv).

🧱 Estructura de Carpetas:

/dags
  └── etl_multi_config_dag.py          ← DAG de Airflow

/scripts
  └── etl_multi_config.py              ← Script ETL dinámico

/data
  ├── pendientes/                      ← CSVs por procesar
  ├── procesado/                       ← CSVs procesados con éxito
  ├── error/                           ← CSVs con errores
  └── config/
      └── archivo.json                 ← Configuraciones específicas por archivo

/logs
  └── etl_multi_log.csv                ← Log con estado por archivo

Cada archivo CSV debe tener un .json con el mismo nombre (sin la extensión) dentro de /data/config/.

🧪 Validaciones implementadas
	•	Campos requeridos (validar_no_nulos)
	•	Tipos de datos esperados (int, float, str)
	•	Rango de enteros compatible con BIGINT
	•	Inserción única por id (ON CONFLICT DO NOTHING)
	•	Registro de errores en la tabla users_rechazados con detalle del motivo

🐘 Base de Datos
	•	PostgreSQL en AWS RDS
	•	Tablas:
	•	users (carga principal)
	•	users_rechazados (datos inválidos + motivo del rechazo)

🐳 Infraestructura
	•	Apache Airflow 2.8.2 (Docker)
	•	Python 3.x
	•	PostgreSQL en AWS
	•	Docker Compose para levantar servicios (no incluido en repo por seguridad)
	•	.env y archivos sensibles no se suben por buenas prácticas

🚀 Ejecución del DAG

Este DAG no tiene schedule_interval definido. Se ejecuta bajo demanda desde la UI de Airflow.

python /opt/airflow/scripts/etl_multi_config.py

Esto se ejecuta internamente mediante el BashOperator definido en el DAG.

✅ Resultados Esperados
	•	Archivos válidos son cargados en PostgreSQL.
	•	Archivos con errores son movidos y logueados.
	•	El proceso es flexible: puedes añadir más archivos .csv + .json sin modificar el código.
	•	Proyecto listo para escalar hacia nuevos datasets o formatos.



