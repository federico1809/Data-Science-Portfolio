-- QUERY 9: Costo de mantenimiento por km (uso de CTE)
explain analyze
with vehicle_km as (   -- Se abre un CTE (Common Table Expression) llamado "vehicle_km".
                             -- Un CTE es como crear una tabla temporal lógica disponible para la query principal.
select
t.vehicle_id,    -- Se agrupan los kilómetros recorridos por cada vehículo.
sum(r.distance_km) as total_km   -- SUM() acumula la distancia de todas las rutas hechas por ese vehículo
from trips t      -- Tabla de viajes. Alias "t".
join routes r           -- Se unen rutas y viajes para saber cuánto mide cada ruta.
	on r.route_id = t.route_id  -- Condición de join: la ruta del viaje.
group by t.vehicle_id      -- Obligatorio porque se usa SUM(); agrupa por vehículo. 
),
maintenance_cost as (   -- Segundo CTE, ahora calculando costos de mantenimiento.
select
m.vehicle_id,                  -- Se agrupan los costos por vehículo.
sum(m.cost) as total_maintenance_cost -- Total gastado en mantenimiento por cada vehículo.
from maintenance m             -- Tabla de mantenimientos. Alias "m".
group by m.vehicle_id   -- Se usa SUM(); se debe agrupar por vehículo.
)
select
veh.vehicle_id,       -- ID del vehículo tomado de la tabla "vehicles".
veh.vehicle_type,     -- Tipo de vehículo (camión, furgón, etc.)
mc.total_maintenance_cost, -- Costo total de mantenimiento desde el CTE.
vk.total_km,         -- Kilómetros totales desde el CTE.
round(
	mc.total_maintenance_cost / nullif(vk.total_km,0), 2) 
as cost_per_km                          -- Cálculo clave:
                              -- costo_total / km_recorridos /// NULLIF(x,0) evita división por cero:
                              -- si los km son 0 devuelve NULL.
                              -- ROUND(.., 2) redondea a 2 decimales.
from vehicles veh       -- Tabla principal de vehículos.
join vehicle_km vk       -- Une el primer CTE
	on vk.vehicle_id = veh.vehicle_id  -- Necesario porque el CTE solo tiene vehículos que hicieron viajes.      
join maintenance_cost mc       -- Une el segundo CTE
	on mc.vehicle_id = veh.vehicle_id  -- Solo suma vehículos que tienen mantenimiento registrado.
order by cost_per_km desc;    -- Ordena del más costoso al menos costoso por kilómetro.
-- El mayor cuello estaba en la tabla maintenance, sin índice sobre vehicle_id.
-- Con el índice idx_maintenance_vehicle_cost, el Hash Join se convirtió en Index Scan,
-- reduciendo en gran medida el tiempo de ejecución.