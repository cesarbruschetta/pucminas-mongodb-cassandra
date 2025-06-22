import logging
import json
import jsonlines
from cassandra.query import BatchStatement, ConsistencyLevel

from mongodb_cassandra.utils import (
    get_decimal_value,
    get_timestamp_value,
    BASE_DIR,
)

logger = logging.getLogger(__name__)

class Comments:
    def __init__(self, body, email, date):
        self.body = body
        self.email = email
        self.date = date


def import_data_in_cassandra(cluster):
    KEYSPACE_NAME = "sample_training"
    session = cluster.connect()
    session.set_keyspace(KEYSPACE_NAME)
    logger.info(f"Select Keyspace: {KEYSPACE_NAME}")

    # Prepare a instrução de inserção para melhor performance e segurança
    insert_listing_cql = session.prepare(
        """
       INSERT INTO sample_training.posts
        ( 
            id, body, comments, date
        ) VALUES ( ?, ?, ?, ? );
        
    """
    )
    insert_listing_cql.consistency_level = (
        ConsistencyLevel.QUORUM
    )
    batch = BatchStatement()
    processed_count = 0
    batch_size = 1  # Número de inserções por batch

    with jsonlines.open(
        BASE_DIR / "data/sample_training.posts.json", 'r'
    ) as reader:
        for post in reader:
            try:
                # --- Extração e Conversão de Campos Top-Level ---
                post_id = post.get('_id')
                if not post_id:
                    print(f"Skipping record due to missing _id: {post}")
                    continue

                body = post.get('body')
                date = get_timestamp_value(post.get('date'))

                comments = post.get('comments', [])
                comments_list = []
                for comment in comments:
                    body = comment.get('body')
                    email = comment.get('email')
                    author = comment.get('author')
                    comments_list.append(Comments(body, email, author))

                # --- Montar os parâmetros para a inserção ---
                params = (
                    post_id, body, comments_list, date
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
    logger.info(
        f"Inserção final: {processed_count} listagens processadas no total."
    )
