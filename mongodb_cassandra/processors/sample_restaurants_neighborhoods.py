import logging
import json
from cassandra.query import BatchStatement, ConsistencyLevel

from mongodb_cassandra.utils import (
    BASE_DIR,
)

logger = logging.getLogger(__name__)


class Geometry:
    def __init__(self, type, coordinates):
        self.type = type
        self.coordinates = coordinates


def import_data_in_cassandra(cluster):
    KEYSPACE_NAME = "sample_restaurants"
    session = cluster.connect()
    session.set_keyspace(KEYSPACE_NAME)
    logger.info(f"Select Keyspace: {KEYSPACE_NAME}")

    # Prepare a instrução de inserção para melhor performance e segurança
    insert_listing_cql = session.prepare(
        """
       INSERT INTO sample_restaurants.neighborhoods
        ( 
            id, geometry, name
        ) VALUES ( ?, ?, ?);
    """
    )
    insert_listing_cql.consistency_level = ConsistencyLevel.QUORUM
    batch = BatchStatement()
    processed_count = 0
    batch_size = 1  # Número de inserções por batch

    with open(
        BASE_DIR / "data/sample_restaurants.neighborhoods.json", "r"
    ) as json_file:
        reader = json.load(json_file)
        for post in reader:
            try:
                # --- Extração e Conversão de Campos Top-Level ---
                post_id = post.get("_id", {}).get("$oid")
                if not post_id:
                    print(f"Skipping record due to missing _id: {post}")
                    continue

                geometry_data = post.get("geometry", {})
                geometry_type = geometry_data.get("type")
                coordinates = geometry_data.get("coordinates", [])
                geometry = Geometry(geometry_type, coordinates)
                name = post.get("name")

                # --- Montar os parâmetros para a inserção ---
                params = (post_id, geometry, name)

                batch.add(insert_listing_cql, params)
                processed_count += 1

                if processed_count % batch_size == 0:
                    session.execute(batch)
                    batch = BatchStatement()
                    logger.info(f"Inseridos {processed_count} listagens...")

            except Exception as e:
                logger.info(f"Erro ao processar a listagem {post.get('_id')}: {e}")

    # Executa qualquer batch restante
    session.execute(batch)
    logger.info(f"Inserção final: {processed_count} listagens processadas no total.")
