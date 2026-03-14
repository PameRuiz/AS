#!/usr/bin/env python3

import time
import socket
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

# Configuración
CASSANDRA_HOST = '127.0.0.1'
CASSANDRA_PORT = 30042
KEYSPACE = 'space'
TABLE = 'data'

# Identificador del proceso: nombre del host/pod
WHO = socket.gethostname()

def get_current_counter(session):
    """Obtiene el contador global actual (total de filas en la tabla)."""
    row = session.execute(f"SELECT COUNT(*) FROM {KEYSPACE}.{TABLE}").one()
    return row[0] + 1  # +1 porque vamos a insertar una nueva fila

def main():
    print(f"Conectando a Cassandra en {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        load_balancing_policy=RoundRobinPolicy()
    )
    session = cluster.connect()
    print("Conexión establecida.")
   # Crear keyspace si no existe

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS space
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
    """)

    print(f"Keyspace '{KEYSPACE}' listo.")

    # Crear tabla
    session.execute("""
        CREATE TABLE IF NOT EXISTS space.data (
            who text,
            when timestamp,
            counter int,
            attempt int,
            PRIMARY KEY (who, when)
        )
    """)

    # Prepared statement con prefijo completo
    insert_stmt = session.prepare(f"""
        INSERT INTO {KEYSPACE}.{TABLE} (who, when, counter, attempt)
        VALUES (?, ?, ?, ?)
    """)

    attempt = 0  # Contador local de intentos para este proceso

    print(f"Iniciando inserciones como '{WHO}'. Ctrl+C para detener.\n")
    print(f"{'WHO':<20} {'WHEN':<30} {'COUNTER':<10} {'ATTEMPT':<10}")
    print("-" * 70)

    while True:
        attempt += 1  # Incrementar intento local siempre, aunque falle
        try:
            now = datetime.utcnow()
            counter = get_current_counter(session)

            session.execute(insert_stmt, (WHO, now, counter, attempt))

            print(f"{WHO:<20} {str(now):<30} {counter:<10} {attempt:<10}")

        except Exception as e:
            print(f"[ERROR] Intento {attempt} fallido: {e}")
            counter -= 1  # Decrementar el contador local si falla la inserción
            cluster = Cluster(
                        [CASSANDRA_HOST],
                        port=CASSANDRA_PORT,
                        load_balancing_policy=RoundRobinPolicy()
            )
            session = cluster.connect()

        time.sleep(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nPrograma detenido.")
