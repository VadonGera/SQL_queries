Модуль 3. Неделя 5.

Создание SQL-запросов к базе данных и работа с ней посредством библиотеки `psycopg2`

Использованы команды DDL:
* `CREATE`
* `DROP` 

Использованы команды DML:
* `INSERT` 
* `SELECT` 

Основные компоненты SQL-запроса:
* `SELECT` 
* `FROM` 
* `JOIN`
* `WHERE` 
* `GROUP BY`
* `HAVING`
* `ORDER BY`

Использованы инструкции при создании таблиц:
* `PRIMARY KEY`
* `FOREIGN KEY`
* `ON DELETE CASCADE`
* `REFERENCES`

Создание, удаление и применение в таблицах:
* `FUNCTION` 
* `PROCEDURE`


В модуле `main.py`: 
1. происходит подключение в базе PosrgreSQL 
по параметрам из `config.py` 
2. создаются таблицы `customers`, 
`orders`, `product_categories`, `products` и `order_details`, 
3. таблицы заполняются тестовыми данными, 
4. проверяется их заполнение через SELECT запрос,
5. создаются функция `get_sales_category_period` для получения общей 
суммы продаж по категориям товаров за определенный период и с 
использованием SELECT-запроса проверяется ее выполнение,
6. создается процедура `update_stock` для обновления количества 
товара на складе после создания нового заказа и выводится результат
ее применения.
