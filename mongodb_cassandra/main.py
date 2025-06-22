import logging
from cassandra.cluster import Cluster

from mongodb_cassandra.processors import sample_airbnb_listingsAndReviews


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)
CASSANDRA_IP = "192.168.2.129"


def run_processor():
    """
    Função principal para executar o processamento dos dados.
    """
    try:
        # Conecta ao Cassandra
        cluster = Cluster([CASSANDRA_IP])
        logger.info(f"Conectado ao Cassandra em {CASSANDRA_IP}")

        # Executa o processamento
        logger.info("Iniciando o processamento dos dados de listingsAndReviews do MongoDB para o Cassandra.")
        sample_airbnb_listingsAndReviews.import_data_in_cassandra(cluster)

    except Exception as e:
        logger.error(f"Erro geral: {e}")

    finally:
        if "cluster" in locals() and cluster:
            cluster.shutdown()
            logger.info("Conexão com Cassandra fechada.")


if __name__ == "__main__":
    run_processor()
