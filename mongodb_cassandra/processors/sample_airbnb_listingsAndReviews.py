import logging
import jsonlines
from cassandra.query import BatchStatement, ConsistencyLevel

from mongodb_cassandra.utils import (
    get_decimal_value,
    get_timestamp_value,
    BASE_DIR,
)

logger = logging.getLogger(__name__)


class Reviews:
    def __init__(
        self, id, date_review, listing_id, reviewer_id, reviewer_name, comments
    ):
        self.id = id
        self.date_review = date_review
        self.listing_id = listing_id
        self.reviewer_id = reviewer_id
        self.reviewer_name = reviewer_name
        self.comments = comments


def import_data_in_cassandra(cluster):
    KEYSPACE_NAME = "sample_airbnb"
    session = cluster.connect()
    session.set_keyspace(KEYSPACE_NAME)
    logger.info(f"Select Keyspace: {KEYSPACE_NAME}")

    # Prepare a instrução de inserção para melhor performance e segurança
    insert_listing_cql = session.prepare(
        """
       INSERT INTO sample_airbnb.listingsandreviews
        ( 
            id, listing_url, name, summary, space, "description", 
            neighborhood_overview, price, amenities, reviews
        ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? );
        
    """
    )
    insert_listing_cql.consistency_level = ConsistencyLevel.QUORUM
    batch = BatchStatement()
    processed_count = 0
    batch_size = 1  # Número de inserções por batch

    with jsonlines.open(
        BASE_DIR / "data/sample_airbnb.listingsAndReviews.json", "r"
    ) as reader:
        for listing in reader:
            try:
                # --- Extração e Conversão de Campos Top-Level ---
                listing_id = listing.get("_id")
                if not listing_id:
                    print(f"Skipping record due to missing _id: {listing}")
                    continue

                listing_url = listing.get("listing_url")
                name = listing.get("name")
                description = listing.get("description")
                summary = listing.get("summary")
                space = listing.get("space")
                neighborhood_overview = listing.get("neighborhood_overview")
                price = get_decimal_value(listing, "price")
                amenities = set(listing.get("amenities", []))

                reviews_list_for_cassandra = []
                for review_json in listing.get("reviews", [])[
                    :10
                ]:  # Limita a 10 reviews
                    review_data_for_udt = Reviews(
                        **{
                            "id": review_json.get("_id"),
                            "date_review": get_timestamp_value(review_json, "date"),
                            "listing_id": review_json.get("listing_id"),
                            "reviewer_id": review_json.get("reviewer_id"),
                            "reviewer_name": review_json.get("reviewer_name"),
                            "comments": review_json.get("comments"),
                        }
                    )
                    reviews_list_for_cassandra.append(
                        review_data_for_udt
                    )  # Adiciona o dicionário diretamente

                # --- Montar os parâmetros para a inserção ---
                params = (
                    listing_id,
                    listing_url,
                    name,
                    summary,
                    space,
                    description,
                    neighborhood_overview,
                    price,
                    amenities,
                    tuple(reviews_list_for_cassandra),
                )

                batch.add(insert_listing_cql, params)
                processed_count += 1

                if processed_count % batch_size == 0:
                    session.execute(batch)
                    batch = BatchStatement()
                    logger.info(f"Inseridos {processed_count} listagens...")

            except Exception as e:
                logger.info(f"Erro ao processar a listagem {listing.get('_id')}: {e}")

    # Executa qualquer batch restante
    session.execute(batch)
    logger.info(f"Inserção final: {processed_count} listagens processadas no total.")
