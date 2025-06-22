-- Cria o Keyspace
CREATE KEYSPACE IF NOT EXISTS sample_training
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Seleciona o Keyspace
USE sample_training;

-- UDT para informações de localização de escritório
CREATE TYPE IF NOT EXISTS office_location_udt (
    latitude double,
    longitude double
);

-- UDT para detalhes de escritório
CREATE TYPE IF NOT EXISTS office_udt (
    description text,
    address1 text,
    address2 text,
    zip_code text,
    city text,
    state_code text,
    country_code text,
    location frozen<office_location_udt>
);

CREATE TYPE IF NOT EXISTS funding_round_udt (
    round_code text,
    raised_amount int,
    currency_code text,
    funded_year int,
    funded_month int,
    funded_day int
);

-- Cria a tabela principal para as empresas
CREATE TABLE IF NOT EXISTS companies (
    id text PRIMARY KEY,
    name text,
    permalink text,
    twitter_username text,
    description text,
    founded_year int,
    offices list<frozen<office_udt>>,
    funding_rounds list<frozen<funding_round_udt>>,
);