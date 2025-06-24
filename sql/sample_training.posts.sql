-- Cria o Keyspace
CREATE KEYSPACE IF NOT EXISTS sample_training
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Seleciona o Keyspace
USE sample_training;

-- UDT para comentários
CREATE TYPE IF NOT EXISTS comment_udt (
    body text,
    email text,
    author text
);

-- Cria a tabela principal para os posts
CREATE TABLE IF NOT EXISTS sample_training.posts (
    id text PRIMARY KEY,
    body text,
    permalink text,
    author text,
    title text,
    tags set<text>,
    comments list<frozen<comment_udt>>,
    date timestamp
);

CREATE INDEX IF NOT EXISTS on_post_author ON posts (author);
CREATE INDEX IF NOT EXISTS on_post_tags ON posts (tags);