# ​ Proyecto ETL con PySpark – Usuarios CSV

*Fecha de implementación:* Agosto 2025  
*Autor:* Geovane Pérez

---

##  Objetivo

Desarrollar un pipeline ETL automatizado con PySpark que:
- Cargue data desde un archivo usuarios.csv
- Limpie, transforme y valide datos
- Genere estadísticas descriptivas
- Exporte resultados en formatos fáciles de consumir

---

##  Contenidos del proyecto

| Archivo                      | Descripción                                                         |
|-----------------------------|----------------------------------------------------------------------|
| usuarios.csv              | Datos de entrada originales                                          |
| etl_spark.ipynb           | Notebook con todo el procesamiento paso a paso                      |
| usuarios_transformados.csv| Salida con las columnas nuevas agregadas                            |
| estadisticas_ciudad.csv   | Agregados por ciudad (conteo y promedio de edad)                    |
| comandos_spark.txt        | Guía con todos los comandos Spark usados y su explicación           |

---

##  Flujo de trabajo

1. Se carga el CSV con spark.read.csv() en un DataFrame.
2. Se limpian columnas, se agregan columnas derivadas como es_adulto y rango_edad.
3. Se realizan cálculos agregados por ciudad: conteo total y edad promedio.
4. Se exportan los resultados:
   - usuarios_transformados.csv
   - estadisticas_ciudad.csv
5. Todo queda documentado en el notebook y el archivo de comandos.

---

##  Estructura del repositorio

spark-etl-usuarios/
├── usuarios.csv
├── etl_spark.ipynb
├── MINI PROYECTO SPARK ANÁLISIS DE USUARIOS.txt
├── usuarios_transformados.csv
├── estadisticas_ciudad.csv
├── comandos_spark.txt
└── README.md

---

##  Aprendizajes

- Uso de PySpark (.read.csv(), .withColumn(), .filter(), .groupBy(), .write)
- Manipulación distribuida de datos
- Exportación a CSV desde Spark
- Documentación clara para reproducibilidad

---

##  Próximos pasos (opcional)

- Guardar resultados en Parquet para eficiencia
- Comparar tiempos vs Pandas cuando los datos escalen
- Convertir este pipeline en un DAG de Airflow si lo requieres

---

##  Contacto

- GitHub: [Geovane-Perez](https://github.com/Geovane-Perez)  
- Correo: geovane.perez.96@hotmail.com
