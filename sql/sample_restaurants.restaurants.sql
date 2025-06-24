CREATE KEYSPACE IF NOT EXISTS sample_restaurants 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sample_restaurants;

-- UDT para o endereço do restaurante
CREATE TYPE IF NOT EXISTS address_udt (
    building text,
    coord list<double>,
    street text,
    zipcode text
);

-- UDT para cada registro de nota/grade
CREATE TYPE IF NOT EXISTS grade_udt (
    date timestamp,
    grade text,
    score int
);

CREATE TABLE IF NOT EXISTS restaurants (
    id text PRIMARY KEY,
    address frozen<address_udt>,
    borough text,
    cuisine text,
    grades list<frozen<grade_udt>>,
    name text,
    restaurant_id text
);

CREATE INDEX IF NOT EXISTS on_restaurant_name ON restaurants (name);
CREATE INDEX IF NOT EXISTS on_restaurant_borough ON restaurants (borough);
