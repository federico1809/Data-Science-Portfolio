-- Base de Datos: VentasDB

-- Eliminamos tablas si existen (Sintaxis MSSQL para IF EXISTS)
IF OBJECT_ID('audit_logs') IS NOT NULL DROP TABLE audit_logs;
IF OBJECT_ID('order_items') IS NOT NULL DROP TABLE order_items;
IF OBJECT_ID('orders') IS NOT NULL DROP TABLE orders;
IF OBJECT_ID('products') IS NOT NULL DROP TABLE products;
IF OBJECT_ID('customers') IS NOT NULL DROP TABLE customers;

-- Tabla: customers
CREATE TABLE customers (
    -- CAMBIO CLAVE: SERIAL PRIMARY KEY se reemplaza por IDENTITY(1,1) PRIMARY KEY
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    created_at DATE
);

-- Tabla: products
CREATE TABLE products (
    -- CAMBIO CLAVE: IDENTITY(1,1)
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);

-- Tabla: orders
CREATE TABLE orders (
    -- CAMBIO CLAVE: IDENTITY(1,1)
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date DATE
);

-- Tabla: order_items
CREATE TABLE order_items (
    -- CAMBIO CLAVE: IDENTITY(1,1)
    order_item_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT
);

-- Tabla: audit_logs
CREATE TABLE audit_logs (
    -- CAMBIO CLAVE: IDENTITY(1,1)
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    event_type VARCHAR(50),
    event_timestamp DATETIME, -- TIMESTAMP en MSSQL es diferente, usamos DATETIME
    user_info VARCHAR(100)
);

-- Datos iniciales (El DML es el mismo)
SET IDENTITY_INSERT customers OFF; -- Aseguramos que la inserción de datos se haga sin especificar ID
INSERT INTO customers (full_name, email, created_at) VALUES
('Carlos Pérez', 'carlos.perez@email.com', '2024-01-10'),
('Lucía Gómez', 'lucia.gomez@email.com', '2024-02-05'),
('Martín Silva', 'martin.silva@email.com', '2024-03-12');

INSERT INTO products (product_name, category, price) VALUES
('Teclado Mecánico', 'Periféricos', 85.50),
('Monitor 24"', 'Pantallas', 220.00),
('Mouse Inalámbrico', 'Periféricos', 45.90);

INSERT INTO orders (customer_id, order_date) VALUES
(1, '2024-04-01'),
(2, '2024-04-03'),
(1, '2024-04-10');

INSERT INTO order_items (order_id, product_id, quantity) VALUES
(1, 1, 2),
(1, 3, 1),
(2, 2, 1),
(3, 1, 1),
(3, 2, 1);

INSERT INTO audit_logs (event_type, event_timestamp, user_info) VALUES
('LOGIN', '2024-04-01 08:00:00', 'admin'),
('INSERT_ORDER', '2024-04-01 08:10:15', 'carlos.perez'),
('INSERT_ORDER', '2024-04-03 09:00:30', 'lucia.gomez'),
('LOGIN', '2024-04-10 07:50:00', 'admin');