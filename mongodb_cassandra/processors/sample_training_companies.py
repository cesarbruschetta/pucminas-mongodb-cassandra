import logging
import json
import jsonlines
from cassandra.query import BatchStatement, ConsistencyLevel

from mongodb_cassandra.utils import (
    get_float_value,
    get_int_value,
    BASE_DIR,
)

logger = logging.getLogger(__name__)

class OfficeLocation:
    def __init__(self,latitude, longitude):
        self.latitude = latitude
        self.longitude = longitude

class Office:
     def __init__(self, description, address1, address2, zip_code, city, state_code, country_code, location):
        self.description = description
        self.address1 = address1
        self.address2 = address2
        self.zip_code = zip_code
        self.city = city
        self.state_code = state_code
        self.country_code = country_code
        self.location = location


class FundingRound:
    def __init__(self, round_code, raised_amount, currency_code, funded_year, funded_month, funded_day):
        self.round_code = round_code
        self.raised_amount = raised_amount
        self.currency_code = currency_code
        self.funded_year = funded_year
        self.funded_month = funded_month
        self.funded_day = funded_day


def import_data_in_cassandra(cluster):
    KEYSPACE_NAME = "sample_training"
    session = cluster.connect()
    session.set_keyspace(KEYSPACE_NAME)
    logger.info(f"Select Keyspace: {KEYSPACE_NAME}")

    # Prepare a instrução de inserção para melhor performance e segurança
    insert_listing_cql = session.prepare(
        """
       INSERT INTO sample_training.companies
        ( 
            id, name, permalink, twitter_username, description, 
            founded_year, offices, funding_rounds
        ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)
        
    """
    )
    insert_listing_cql.consistency_level = (
        ConsistencyLevel.QUORUM
    )
    batch = BatchStatement()
    processed_count = 0
    batch_size = 1  # Número de inserções por batch

    with jsonlines.open(
        BASE_DIR / "data/sample_training.companies.json", 'r'
    ) as reader:
        for company in reader:
            try:
                # --- Extração e Conversão de Campos Top-Level ---
                company_id = company.get('_id', {}).get('$oid')
                if not company_id:
                    print(f"Skipping record due to missing _id: {company}")
                    continue

                
                name = company.get('name')
                permalink = company.get('permalink')
                twitter_username = company.get('twitter_username')
                description = company.get('description')
                founded_year = get_int_value(company, 'founded_year')

                offices = company.get('offices', [])
                offices_list = []
                for office in offices:
                    
                    description = office.get('description')
                    address1 = office.get('address1')
                    address2 = office.get('address2')
                    zip_code = office.get('zip_code')
                    state_code = office.get('state_code')
                    country_code = office.get('country_code')
                    location = office.get('location', {})

                    latitude = get_float_value(location, 'latitude')
                    longitude = get_float_value(location, 'longitude')

                    offices_list.append(Office(**{
                        'description': description,
                        'address1': address1,
                        'address2': address2,
                        'zip_code': zip_code,
                        'city': office.get('city'),
                        'state_code': state_code,
                        'country_code': country_code,
                        'location': OfficeLocation(latitude, longitude)
                    }))


                funding_rounds = company.get('funding_rounds', [])
                funding_rounds_list = []
                for funding_round in funding_rounds:
                    round_code = funding_round.get('round_code')
                    raised_amount = get_int_value(funding_round, 'raised_amount')
                    currency_code = funding_round.get('currency_code')
                    funded_year = get_int_value(funding_round, 'funded_year')
                    funded_month = get_int_value(funding_round, 'funded_month')
                    funded_day = get_int_value(funding_round, 'funded_day')

                    funding_rounds_list.append(FundingRound(
                        round_code=round_code,
                        raised_amount=raised_amount,
                        currency_code=currency_code,
                        funded_year=funded_year,
                        funded_month=funded_month,
                        funded_day=funded_day
                    ))

                # --- Montar os parâmetros para a inserção ---
                params = (
                    company_id, name, permalink, twitter_username, description,
                    founded_year, offices_list, funding_rounds_list
                )

                batch.add(insert_listing_cql, params)
                processed_count += 1

                if processed_count % batch_size == 0:
                    session.execute(batch)
                    batch = BatchStatement()
                    logger.info(f"Inseridos {processed_count} listagens...")

            except Exception as e:
                logger.info(f"Erro ao processar a listagem {company_id}: {e}")

    # Executa qualquer batch restante
    session.execute(batch)
    logger.info(
        f"Inserção final: {processed_count} listagens processadas no total."
    )
