import shutil
import os

def mover_archivo_procesado():
    origen = '/opt/airflow/data/sample_data.csv'
    destino = '/opt/airflow/procesado/'
    
    if os.path.exists(origen):
        shutil.move(origen, destino)
        print(f'Archivo movido de {origen} a {destino}')
    else:
        print('Archivo no encontrado para mover.')