# Projeto para migração de dados do MongoDB para Cassandra

## Objetivo
O objetivo deste projeto é demonstrar como migrar dados de um banco de dados MongoDB para um banco de dados Cassandra.

## Comando para criar o keyspace

```sql
cqlsh 192.168.2.129 -f ./sql/sample_airbnb.listingsAndReviews.sql
cqlsh 192.168.2.129 -f ./sql/sample_restaurants.neighborhoods.sql
cqlsh 192.168.2.129 -f ./sql/sample_restaurants.restaurants.sql
cqlsh 192.168.2.129 -f ./sql/sample_training.companies.sql
cqlsh 192.168.2.129 -f ./sql/sample_training.posts.sql
```