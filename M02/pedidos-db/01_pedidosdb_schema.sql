-- Crear esquema
CREATE SCHEMA pedidosdb;

-- Tabla de hechos: fact_pedidos
CREATE TABLE pedidosdb.fact_pedidos (
    id_pedido INT PRIMARY KEY,
    id_cliente INT,
    id_restaurante INT,
    id_fecha INT,
    monto_total NUMERIC(10,2)
);

-- Dimensión: dim_clientes
CREATE TABLE pedidosdb.dim_clientes (
    id_cliente INT PRIMARY KEY,
    ciudad VARCHAR(100),
    tipo_usuario VARCHAR(20),
    dispositivo VARCHAR(20)
);

-- Dimensión: dim_restaurantes
CREATE TABLE pedidosdb.dim_restaurantes (
    id_restaurante INT PRIMARY KEY,
    nombre VARCHAR(100),
    tipo_comida VARCHAR(50),
    calificacion NUMERIC(3,1)
);

-- Dimensión: dim_fechas_pedidos
CREATE TABLE pedidosdb.dim_fechas_pedidos (
    id_fecha INT PRIMARY KEY,
    dia INT,
    mes INT,
    año INT,
    dia_semana VARCHAR(20)
);
