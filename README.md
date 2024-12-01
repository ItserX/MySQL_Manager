GET / - возвращает список все таблиц
GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
GET /table/table/table/id - возвращает информацию о самой записи
PUT /$table - создаёт новую запись, данный по записи в теле запроса 
POST /table/table/table/id - обновляет запись, данные приходят в теле запроса
DELETE /table/table/table/id - удаляет запись

Проверка работы с mysql базой данной локально.
1. sudo docker run -p 3306:3306 -v $(pwd)/mysql-init:/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
2. sudo docker exec -it f5195c842c38 mysql -u root -p