# Projeto para migração de dados do MongoDB para Cassandra


## Descrição


## Comando para criar o keyspace

```sql
cqlsh 192.168.2.129 -f ./sql/sample_airbnb.listingsAndReviews.sql
cqlsh 192.168.2.129 -f ./sql/sample_training.companies.sql
cqlsh 192.168.2.129 -f ./sql/sample_training.posts.sql
```