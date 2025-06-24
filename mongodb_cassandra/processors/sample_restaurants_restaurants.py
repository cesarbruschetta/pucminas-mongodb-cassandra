import logging
import json
from cassandra.query import BatchStatement, ConsistencyLevel

from mongodb_cassandra.utils import (
    get_timestamp_value,
    BASE_DIR,
)

logger = logging.getLogger(__name__)


class Address:
    def __init__(self, building, coord, street, zipcode):
        self.building = building
        self.coord = coord  # Lista de coordenadas [longitude, latitude]
        self.street = street
        self.zipcode = zipcode


class Grade:
    def __init__(self, date, grade, score):
        self.date = date
        self.grade = grade
        self.score = score


def import_data_in_cassandra(cluster):
    KEYSPACE_NAME = "sample_restaurants"
    session = cluster.connect()
    session.set_keyspace(KEYSPACE_NAME)
    logger.info(f"Select Keyspace: {KEYSPACE_NAME}")

    # Prepare a instrução de inserção para melhor performance e segurança
    insert_listing_cql = session.prepare(
        """
       INSERT INTO sample_restaurants.restaurants
        ( 
            id, address, borough, cuisine, grades, name, restaurant_id
        ) VALUES ( ?, ?, ?, ?, ?, ?, ? );
    """
    )
    insert_listing_cql.consistency_level = ConsistencyLevel.QUORUM
    batch = BatchStatement()
    processed_count = 0
    batch_size = 1  # Número de inserções por batch

    with open(BASE_DIR / "data/sample_restaurants.restaurants.json", "r") as json_file:
        reader = json.load(json_file)
        for post in reader:
            try:
                # --- Extração e Conversão de Campos Top-Level ---
                post_id = post.get("_id", {}).get("$oid")
                if not post_id:
                    print(f"Skipping record due to missing _id: {post}")
                    continue

                address_data = post.get("address", {})
                building = address_data.get("building")
                coord = address_data.get("coord", [])
                street = address_data.get("street")
                zipcode = address_data.get("zipcode")
                address = Address(building, coord, street, zipcode)

                grades_data = post.get("grades", [])
                grades = [
                    Grade(
                        date=get_timestamp_value(grade, "date"),
                        grade=grade.get("grade"),
                        score=grade.get("score"),
                    )
                    for grade in grades_data
                ]

                borough = post.get("borough")
                cuisine = post.get("cuisine")
                name = post.get("name")
                restaurant_id = post.get("restaurant_id")

                # --- Montar os parâmetros para a inserção ---
                params = (
                    post_id,
                    address,
                    borough,
                    cuisine,
                    grades,
                    name,
                    restaurant_id,
                )

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
