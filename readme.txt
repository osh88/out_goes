Плагин fluentbit для вывода данных в ElasticSearch.
Умеет класть записи в индекс по шаблону на основе данных в записи.

Пример конфига:
[OUTPUT]
    Name     goes
    Match    *
    Addr     localhost:9200
    Index    docker-{HOSTNAME}-{CONTAINER_NAME}-{CONTAINER_ID}-{ts#timestamp}
    BulkSize 5000
    Timeout  300
    Debug    2
