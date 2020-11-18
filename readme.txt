Плагин fluentbit для вывода данных в ElasticSearch.
Умеет класть записи в индекс по шаблону на основе данных в записи.

Параметры плагина:
Addr     - Адрес эластика (хост:порт).
Index    - Шаблон индекса.
BulkSize - Максимальный размер пачки, отправляемой в эластик.
Timeout  - Максимальное время ожидания обработки запроса эластиком (в секундах).
Debug    - Вывод отладочной информации (0-выключен, 1,2 - включен).

Пример конфига:

/etc/td-agent-bit/td-agent-bit.conf
-----------------------------------
[OUTPUT]
    Name     goes
    Match    *
    Addr     localhost:9200
    Index    docker-{HOSTNAME}-{CONTAINER_NAME}-{CONTAINER_ID}-{ts#timestamp}
    BulkSize 5000
    Timeout  300
    Debug    2

/etc/td-agent-bit/plugins.conf
------------------------------
[PLUGINS]
    Path /var/lib/td-agent-bit/plugins/out_goes.so

Для примера выше плагин будет искать в записи поля HOSTNAME, CONTAINER_NAME, CONTAINER_ID и ts.
Значения этих полей подставит в шаблон. Из поля, помеченного суффиксом "#timestamp" (ts в
данном примере), будет брать год, месяц и день.
Пример результата: docker_service-server1-my_service-e21b94731d87-2020.11.18

Сборка: go build -buildmode=c-shared -o /var/lib/td-agent-bit/plugins/out_goes.so main.go

Дополнительная информация о go-плагинах для fluentbit:
https://docs.fluentbit.io/manual/development/golang-output-plugins
https://github.com/fluent/fluent-bit-go/tree/master/examples/out_multiinstance
