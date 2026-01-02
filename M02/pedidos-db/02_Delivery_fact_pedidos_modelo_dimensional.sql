
-- Plataforma de delivery — Modelo dimensional inicial
-- Motor objetivo: PostgreSQL 13+
-- Basado en el diseño del documento (fact_pedidos + dim_clientes, dim_restaurantes, dim_fechas_pedidos).
-- Pasos recomendados en el documento: importar CSV, definir PK, y luego FKs entre hechos y dimensiones.
-- (Ver imágenes y ejemplos de ALTER TABLE en el material).

BEGIN;

--------------------------------------------------------------------------------
-- 0) Chequeos de calidad (opcional, ejecuta antes de agregar PK/FK)
--------------------------------------------------------------------------------
-- Nulos y duplicados potenciales en claves
-- (Si cualquiera de estas consultas devuelve filas, corrige antes de aplicar PK/FK).

-- fact_pedidos.id_pedido
SELECT id_pedido
FROM fact_pedidos
WHERE id_pedido IS NULL
UNION ALL
SELECT id_pedido
FROM fact_pedidos
GROUP BY id_pedido
HAVING COUNT(*) > 1;

-- dim_clientes.id_cliente
SELECT id_cliente
FROM dim_clientes
WHERE id_cliente IS NULL
UNION ALL
SELECT id_cliente
FROM dim_clientes
GROUP BY id_cliente
HAVING COUNT(*) > 1;

-- dim_restaurantes.id_restaurante
SELECT id_restaurante
FROM dim_restaurantes
WHERE id_restaurante IS NULL
UNION ALL
SELECT id_restaurante
FROM dim_restaurantes
GROUP BY id_restaurante
HAVING COUNT(*) > 1;

-- dim_fechas_pedidos.id_fecha
SELECT id_fecha
FROM dim_fechas_pedidos
WHERE id_fecha IS NULL
UNION ALL
SELECT id_fecha
FROM dim_fechas_pedidos
GROUP BY id_fecha
HAVING COUNT(*) > 1;

--------------------------------------------------------------------------------
-- 1) Primary Keys (PK) — según el documento
--------------------------------------------------------------------------------
ALTER TABLE fact_pedidos
  ADD CONSTRAINT fact_pedidos_pk PRIMARY KEY (id_pedido);

ALTER TABLE dim_clientes
  ADD CONSTRAINT dim_clientes_pk PRIMARY KEY (id_cliente);

ALTER TABLE dim_restaurantes
  ADD CONSTRAINT dim_restaurantes_pk PRIMARY KEY (id_restaurante);

ALTER TABLE dim_fechas_pedidos
  ADD CONSTRAINT dim_fechas_pedidos_pk PRIMARY KEY (id_fecha);

--------------------------------------------------------------------------------
-- 2) Foreign Keys (FK) — conectar hechos con dimensiones
--------------------------------------------------------------------------------
ALTER TABLE fact_pedidos
  ADD CONSTRAINT fk_fact_clientes
  FOREIGN KEY (id_cliente) REFERENCES dim_clientes(id_cliente);

ALTER TABLE fact_pedidos
  ADD CONSTRAINT fk_fact_restaurantes
  FOREIGN KEY (id_restaurante) REFERENCES dim_restaurantes(id_restaurante);

ALTER TABLE fact_pedidos
  ADD CONSTRAINT fk_fact_fechas
  FOREIGN KEY (id_fecha) REFERENCES dim_fechas_pedidos(id_fecha);

--------------------------------------------------------------------------------
-- 3) Índices recomendados (aceleran joins y filtros habituales)
--------------------------------------------------------------------------------
-- Índices en las columnas FK de la tabla de hechos:
CREATE INDEX IF NOT EXISTS idx_fact_pedidos_cliente      ON fact_pedidos(id_cliente);
CREATE INDEX IF NOT EXISTS idx_fact_pedidos_restaurante  ON fact_pedidos(id_restaurante);
CREATE INDEX IF NOT EXISTS idx_fact_pedidos_fecha        ON fact_pedidos(id_fecha);

-- Índices por atributos de corte frecuentes en dimensiones:
CREATE INDEX IF NOT EXISTS idx_dim_clientes_ciudad       ON dim_clientes(ciudad);
CREATE INDEX IF NOT EXISTS idx_dim_clientes_tipo_usuario ON dim_clientes(tipo_usuario);
CREATE INDEX IF NOT EXISTS idx_dim_restaurantes_tipo     ON dim_restaurantes(tipo_comida);
CREATE INDEX IF NOT EXISTS idx_dim_fechas_anio_mes       ON dim_fechas_pedidos(año, mes);

--------------------------------------------------------------------------------
-- 4) Consultas de validación y ejemplo (star-join)
--------------------------------------------------------------------------------
-- 4.1) Chequear integridad referencial (FKs "huérfanas")
-- Si estas consultas devuelven filas, hay IDs en hechos que no existen en dimensiones.
SELECT fp.id_cliente
FROM fact_pedidos fp
LEFT JOIN dim_clientes dc ON dc.id_cliente = fp.id_cliente
WHERE dc.id_cliente IS NULL
LIMIT 10;

SELECT fp.id_restaurante
FROM fact_pedidos fp
LEFT JOIN dim_restaurantes dr ON dr.id_restaurante = fp.id_restaurante
WHERE dr.id_restaurante IS NULL
LIMIT 10;

SELECT fp.id_fecha
FROM fact_pedidos fp
LEFT JOIN dim_fechas_pedidos df ON df.id_fecha = fp.id_fecha
WHERE df.id_fecha IS NULL
LIMIT 10;

-- 4.2) Ejemplo de análisis: total por ciudad y tipo de comida
SELECT
  dc.ciudad,
  dr.tipo_comida,
  SUM(fp.monto_total)::numeric(14,2) AS total_ciudad_tipo
FROM fact_pedidos fp
JOIN dim_clientes      dc ON dc.id_cliente      = fp.id_cliente
JOIN dim_restaurantes  dr ON dr.id_restaurante  = fp.id_restaurante
GROUP BY 1, 2
ORDER BY 1, 2;

-- 4.3) Ejemplo temporal: total por año-mes y ciudad
SELECT
  df.año,
  df.mes,
  dc.ciudad,
  SUM(fp.monto_total)::numeric(14,2) AS total_mensual
FROM fact_pedidos fp
JOIN dim_fechas_pedidos df ON df.id_fecha = fp.id_fecha
JOIN dim_clientes      dc ON dc.id_cliente = fp.id_cliente
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

COMMIT;

-- Fin del script — Modelo dimensional de pedidos (PK/FK + índices + ejemplos).
