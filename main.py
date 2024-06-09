import psycopg2
from config import host, user, password, db_name, port

db_params = {
    "host": host,
    "user": user,
    "password": password,
    "database": db_name,
    "port": port
}

with psycopg2.connect(**db_params) as connection:
    connection.autocommit = True

    with connection.cursor() as cursor:
        # Версия сервера
        query_select = """SELECT version();"""
        cursor.execute(query_select)
        print(f"Версия сервера: {cursor.fetchone()}")

        # Удаляем таблицы (если есть) customers, orders, product_categories, products и order_details
        query_drop = """DROP TABLE IF EXISTS customers, orders, product_categories, products, order_details CASCADE;"""
        cursor.execute(query_drop)
        connection.commit()
        print("[INFO] Таблицы customers, orders, product_categories, products и order_details удалены.")

        # Создаем таблицу customers
        query_create = """
                            CREATE TABLE IF NOT EXISTS customers (
                            customer_id SERIAL PRIMARY KEY,
                            first_name VARCHAR(50),
                            last_name VARCHAR(50),
                            email VARCHAR(100),
                            phone VARCHAR(20) NOT NULL,
                            address VARCHAR(255));
                        """
        cursor.execute(query_create)
        connection.commit()
        print("[INFO] Таблица customers создана.")

        # Создаем таблицу orders
        query_create = """
                            CREATE TABLE IF NOT EXISTS orders (
                            order_id SERIAL PRIMARY KEY,
                            customer_id INT,
                            order_date DATE,
                            shipping_address VARCHAR(255),
                            order_status VARCHAR(50),
                            FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE);
                        """
        cursor.execute(query_create)
        connection.commit()
        print("[INFO] Таблица orders создана.")

        # Создаем таблицу product_categories
        query_create = """
                            CREATE TABLE IF NOT EXISTS product_categories (
                            category_id SERIAL PRIMARY KEY, 
                            category_name VARCHAR(50) UNIQUE);
                        """
        cursor.execute(query_create)
        connection.commit()
        print("[INFO] Таблица product_categories создана.")

        # Создаем таблицу products
        query_create = """
                            CREATE TABLE IF NOT EXISTS products (
                            product_id SERIAL PRIMARY KEY, 
                            product_name VARCHAR(100), 
                            description TEXT,
                            price NUMERIC (10, 2),
                            stock INT,
                            category_id INT,
                            FOREIGN KEY (category_id) REFERENCES product_categories
                            (category_id) ON DELETE CASCADE);
                        """
        cursor.execute(query_create)
        connection.commit()
        print("[INFO] Таблица products создана.")

        # Создаем таблицу order_details
        query_create = """
                            CREATE TABLE IF NOT EXISTS order_details (
                            order_detail_id SERIAL PRIMARY KEY,
                            order_id INT,
                            product_id INT,
                            quantity INT,
                            unit_price NUMERIC(10, 2),
                            FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
                            FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE);
                        """
        cursor.execute(query_create)
        connection.commit()
        print("[INFO] Таблица order_details создана.")

        # Вставка данных в таблицу product_categories
        query_insert = """
                            INSERT INTO product_categories (category_id, category_name)
                            VALUES
                            (1, 'Smartphones'),
                            (2, 'Laptops'),
                            (3, 'Televisions');
                        """
        cursor.execute(query_insert)
        connection.commit()
        print("[INFO] Вставка данных в таблицу product_categories прошла успешно.")

        # Вставка данных в таблицу products
        query_insert = """
                            INSERT INTO products (product_id, product_name, description, price, stock, category_id)
                            VALUES
                            (1, 'Huawei Pura 70', 'Процессор: HiSilicon Kirin 9000S1', 699.90, 100, 1),
                            (2, 'ITEL A70', 'Процессор: Unisoc T603', 85.20, 100, 1),
                            (3, 'TECNO Camon 30', 'Процессор: MediaTek HELIO G99', 239.10, 100, 1),
                            (4, 'Apple MacBook Pro', 'Процессор: Apple M3 Pro 11 core 4 ГГц', 2599.99, 100, 2),
                            (5, 'Samsung UE43CU7100UXRU', 'Операционная система: Tizen 7.0', 399.00, 50, 3);
                        """
        cursor.execute(query_insert)
        connection.commit()
        print("[INFO] Вставка данных в таблицу customers прошла успешно.")

        # Вставка данных в таблицу customers
        query_insert = """
                            INSERT INTO customers (first_name, last_name, email, phone, address)
                            VALUES
                            ('John', 'Doe', 'john.doe@example.com', '123-456-7890', '123 Elm St'),
                            ('Jane', 'Doe', 'jane.doe@example.com', '987-654-3210', '456 Oak St'),
                            ('Alice', 'Johnson', 'alice.johnson@example.com', '555-678-1234', '789 Pine St'),
                            ('Bob', 'Smith', 'bob.smith@example.com', '555-123-4567', '789 Maple St'),
                            ('Charlie', 'Brown', 'charlie.brown@example.com', '555-987-6543', '321 Chestnut St');
                        """
        cursor.execute(query_insert)
        connection.commit()
        print("[INFO] Вставка данных в таблицу customers прошла успешно.")

        # Вставка данных в таблицу orders
        query_insert = """
                            INSERT INTO orders (customer_id, order_date, shipping_address, order_status)
                            VALUES
                            (1, '2023-01-01', '123 Elm St', 'Shipped'),
                            (2, '2023-01-02', '456 Oak St', 'Pending'),
                            (3, '2023-01-03', '789 Pine St', 'Delivered'),
                            (4, '2023-01-04', '789 Maple St', 'Cancelled'),
                            (5, '2023-01-05', '321 Chestnut St', 'Shipped');
                        """
        cursor.execute(query_insert)
        connection.commit()
        print("[INFO] Вставка данных в таблицу orders прошла успешно.")

        # Вставка данных в таблицу order_details
        query_insert = """
                            INSERT INTO order_details (order_detail_id, order_id, product_id, quantity, unit_price)
                            VALUES
                            (1, 1, 1, 10, 680),
                            (2, 2, 2, 15, 85),
                            (3, 3, 3, 5, 200),
                            (4, 5, 3, 1, 239.10),
                            (5, 5, 4, 1, 2599.99),
                            (6, 5, 5, 1, 399);
                        """
        cursor.execute(query_insert)
        connection.commit()
        print("[INFO] Вставка данных в таблицу order_details прошла успешно.")

        # Проверяем таблицу customers
        query_select = """SELECT * FROM customers;"""
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)

        # Проверяем таблицу orders
        query_select = """SELECT * FROM orders;"""
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)

        # Проверяем таблицу product_categories
        query_select = """SELECT * FROM product_categories;"""
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)

        # Проверка связей FOREIGN KEY
        query_select = """
            SELECT orders.order_id, orders.order_date, orders.shipping_address, 
            orders.order_status, customers.first_name, customers.last_name
            FROM orders
            JOIN customers ON orders.customer_id = customers.customer_id;
        """
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)

        # Проверяем таблицу products
        query_select = """SELECT * FROM products;"""
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)

        # Функция для получения общей суммы продаж по категориям товаров за определенный период.
        create_function = """
            DROP FUNCTION IF EXISTS get_sales_category_period;
                    
            CREATE FUNCTION get_sales_category_period(category_id INT, date_from DATE, date_to DATE) 
            RETURNS NUMERIC(10, 2)
            AS $$
            BEGIN
                RETURN (
                SELECT SUM(order_details.quantity * order_details.unit_price)
                FROM orders INNER JOIN (products INNER JOIN order_details 
                    ON products.product_id = order_details.product_id) 
                    ON orders.order_id = order_details.order_id
                WHERE (orders.order_date >= get_sales_category_period.date_from) 
                    AND (orders.order_date <= get_sales_category_period.date_to)
                GROUP BY products.category_id
                HAVING products.category_id = get_sales_category_period.category_id
                );
            END;
            $$ LANGUAGE plpgsql;
        """
        cursor.execute(create_function)
        connection.commit()
        print()
        print("[INFO] Функция get_sales_category_period создана успешно.")

        # Проверяем функцию get_sales_category_period
        query_select = """
            SELECT category_name, get_sales_category_period(category_id, '2023-01-01', '2023-01-04') AS total
            FROM product_categories
            ORDER BY category_name;
        """
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)

        # Сщздание процедуры для обновления количества товара на складе после создания нового заказа.
        create_call = """
            DROP PROCEDURE IF EXISTS update_stock;
        
            CREATE PROCEDURE update_stock(order_id INT) AS $$
            BEGIN
                UPDATE products
                SET stock = stock - od.quantity
                FROM order_details od
                WHERE od.order_id = update_stock.order_id
                AND products.product_id = od.product_id;
            
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'Заказ % не найден.', update_stock.order_id;
                END IF;
            END;
            $$ LANGUAGE plpgsql;
        """
        cursor.execute(create_call)
        connection.commit()
        print()
        print("[INFO] Процедура update_stock создана успешно.")

        # Проверяем результат выполнения Call update_stock
        run_call = """CALL update_stock(5);"""
        cursor.execute(run_call)
        query_select = """SELECT * FROM products;"""
        cursor.execute(query_select)
        records = cursor.fetchall()
        print()
        print(f"Результат {query_select}:")
        for rec in records:
            print(rec)
