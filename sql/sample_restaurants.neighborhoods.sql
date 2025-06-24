CREATE KEYSPACE IF NOT EXISTS sample_restaurants 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sample_restaurants;

-- UDT para a estrutura de geometria (com coordenadas complexas)
CREATE TYPE IF NOT EXISTS geometry_udt (
    coordinates list<frozen<list<frozen<list<double>>>>>,
    type text
);

CREATE TABLE IF NOT EXISTS neighborhoods (
    id text PRIMARY KEY,
    geometry frozen<geometry_udt>,
    name text
);