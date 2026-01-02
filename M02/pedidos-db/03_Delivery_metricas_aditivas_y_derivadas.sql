
-- Plataforma de delivery — Métricas aditivas y derivadas
-- Motor objetivo: PostgreSQL 13+ (adaptable a otros)
-- Referencias del material: monto_total (aditiva), promedios y distribuciones por dimensiones.
-- Tablas asumidas: fact_pedidos(id_pedido, id_cliente, id_restaurante, id_fecha, monto_total, ...)
--                   dim_clientes(id_cliente, ciudad, ...)
--                   dim_restaurantes(id_restaurante, tipo_comida, ...)
--                   dim_fechas_pedidos(id_fecha, fecha, dia, ...)

BEGIN;
SET search_path TO pedidosdb;

--------------------------------------------------------------------------------
-- 1) Total gastado por cliente (métrica ADITIVA)
--------------------------------------------------------------------------------
SELECT
  id_cliente,
  SUM(monto_total)::numeric(14,2) AS total_gastado
FROM fact_pedidos
GROUP BY id_cliente
ORDER BY total_gastado DESC;

--------------------------------------------------------------------------------
-- 2) Monto promedio por tipo de comida (aditiva, vía dimensión restaurante)
--------------------------------------------------------------------------------
SELECT
  r.tipo_comida,
  AVG(f.monto_total)::numeric(14,2) AS monto_promedio
FROM fact_pedidos f
JOIN dim_restaurantes r
  ON f.id_restaurante = r.id_restaurante
GROUP BY r.tipo_comida
ORDER BY monto_promedio DESC, r.tipo_comida;

--------------------------------------------------------------------------------
-- 3) Distribución del ingreso por ciudad (aditiva, vía dimensión cliente)
--------------------------------------------------------------------------------
SELECT
  c.ciudad,
  SUM(f.monto_total)::numeric(14,2) AS total_ingresos
FROM fact_pedidos f
JOIN dim_clientes c
  ON f.id_cliente = c.id_cliente
GROUP BY c.ciudad
ORDER BY total_ingresos DESC, c.ciudad;

--------------------------------------------------------------------------------
-- 4) Distribución del ingreso por día de la semana (aditiva, vía dimensión fecha)
--------------------------------------------------------------------------------
-- Nota: si quieres ordenar L->D, usa un campo numérico (p.ej., EXTRACT(ISODOW FROM fecha)).
SELECT
  d.dia AS dia_semana,
  SUM(f.monto_total)::numeric(14,2) AS total_ingresos
FROM fact_pedidos f
JOIN dim_fechas_pedidos d
  ON f.id_fecha = d.id_fecha
GROUP BY d.dia
ORDER BY MIN(d.id_fecha);  -- alternativa: ORDER BY COALESCE(d.numero_dia_semana, EXTRACT(ISODOW FROM d.fecha))

--------------------------------------------------------------------------------
-- 5) Ticket promedio por cliente (métrica DERIVADA)
--------------------------------------------------------------------------------
SELECT
  id_cliente,
  (SUM(monto_total) / NULLIF(COUNT(id_pedido), 0))::numeric(14,2) AS ticket_promedio
FROM fact_pedidos
GROUP BY id_cliente
ORDER BY ticket_promedio DESC NULLS LAST;

--------------------------------------------------------------------------------
-- 6) Promedio de pedidos por restaurante y por cliente (derivada)
--------------------------------------------------------------------------------
SELECT
  id_restaurante,
  (COUNT(id_pedido)::numeric / NULLIF(COUNT(DISTINCT id_cliente), 0))::numeric(14,2) AS pedidos_promedio_por_cliente
FROM fact_pedidos
GROUP BY id_restaurante
ORDER BY pedidos_promedio_por_cliente DESC, id_restaurante;

--------------------------------------------------------------------------------
-- 7) (Opcional) Índices recomendados para acelerar las consultas
--------------------------------------------------------------------------------
-- CREATE INDEX IF NOT EXISTS idx_fp_cliente      ON fact_pedidos(id_cliente);
-- CREATE INDEX IF NOT EXISTS idx_fp_restaurante  ON fact_pedidos(id_restaurante);
-- CREATE INDEX IF NOT EXISTS idx_fp_fecha        ON fact_pedidos(id_fecha);
-- CREATE INDEX IF NOT EXISTS idx_dr_tipo_comida  ON dim_restaurantes(tipo_comida);
-- CREATE INDEX IF NOT EXISTS idx_dc_ciudad       ON dim_clientes(ciudad);

COMMIT;

-- Fin del script: métricas aditivas/derivadas.
