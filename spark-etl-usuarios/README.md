# ETL con PySpark - Usuarios CSV

Este proyecto realiza un flujo completo de ETL usando PySpark, trabajando sobre un archivo usuarios.csv con datos ficticios.

## 📌 Objetivos

- Leer un archivo CSV con PySpark
- Limpiar y transformar los datos
- Calcular estadísticas por ciudad
- Exportar archivos transformados

## 📁 Archivos

| Archivo | Descripción |
|--------|-------------|
| usuarios.csv | Archivo original de entrada |
| etl-spark.ipynb | Notebook con todo el procesamiento paso a paso |
| usuarios_transformados.csv | Archivo resultado con nuevas columnas |
| estadisticas_ciudad.csv | Estadísticas por ciudad (promedio de edad, total) |
| comandos_spark.txt | Archivo con todos los comandos explicados |

## 🧠 Qué aprendí

- Uso de .read.csv(), .withColumn(), .filter(), .groupBy()
- Lógica condicional con when()

- Escritura de DataFrames como CSV
