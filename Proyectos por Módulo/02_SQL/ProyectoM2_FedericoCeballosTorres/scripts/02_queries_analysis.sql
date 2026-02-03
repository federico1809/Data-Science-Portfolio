 
 -- INDICES
 -- Para trips (joins con vehicle, driver, route)
CREATE INDEX idx_trips_driver ON trips(driver_id);
CREATE INDEX idx_trips_vehicle ON trips(vehicle_id);
CREATE INDEX idx_trips_route ON trips(route_id);

-- Para deliveries (fechas)
CREATE INDEX idx_deliveries_scheduled 
ON deliveries(scheduled_datetime);

-- Para maintenance (Query 9)
CREATE INDEX idx_maintenance_vehicle_cost 
ON maintenance(vehicle_id, cost);

-- Para conductores en Query 2
CREATE INDEX idx_drivers_license_expiry 
ON drivers(license_expiry);

-- Para conductores activos (Query 5)
CREATE INDEX idx_drivers_status_active 
ON drivers(status)
WHERE status = 'active';

-----
ANALYZE vehicles;
ANALYZE drivers;
ANALYZE routes;
ANALYZE trips;
ANALYZE deliveries;
ANALYZE maintenance;


-- QUERY 1: Contar vehículos por tipo
explain analyze
select vehicle_type,   -- selecciono la columna que clasifica los vehículos
count(*) as total_vehicles  -- cuento cuántos vehículos hay en cada tipo
from vehicles    -- uso la tabla principal de vehículos
group by vehicle_type  -- agrupo por tipo para que COUNT funcione por categoría
order by total_vehicles desc; -- ordeno de mayor a menor cantidad
-- La creación de índices no mejora la consulta.

-- QUERY 2: Conductores con licencia a vencer en 30 días
explain analyze
select license_expiry, license_number, driver_id, first_name, last_name
from drivers
where license_expiry <= current_date + interval '30 days' -- filtro: solo quiero licencias que vencen dentro de los próximos 30 días
order by license_expiry asc;
-- Dado que la condición principal del WHERE depende de license_expiry, se creó el índice idx_drivers_license_expiry.
-- Luego de aplicar el índice, el plan cambió a Index Scan, reduciendo el tiempo en gran medida al ejecutar la consulta.

-- QUERY 3: Total de viajes por estado
explain analyze
select status, count(*) as total_viajes  -- cuántos viajes hay en cada estado
from trips
group by status;
-- La creación de índices no mejora esta consulta. El tiempo se mantiene estable.

-- QUERY 4: Entregas por ciudad (últimos 60 días)
explain analyze
select r.destination_city,   -- ciudad destino, viene desde routes
count(del.delivery_id) as total_deliveries,   -- total de entregas en esa ciudad
sum(del.delivery_id) as total_weight_kg,  
sum(del.package_weight_kg) as total_weight_kg  -- peso total enviado
from deliveries del  -- tabla de entregas
join trips t on del.trip_id = t.trip_id  -- relaciono entrega con su viaje
join routes r on t.route_id = r.route_id  -- relaciono viaje con su ruta
where del.scheduled_datetime >= current_date - interval '60 days'  -- filtro: solo entregas programadas en los últimos 60 días
group by r.destination_city 
order by total_deliveries desc;  -- la ciudad con más entregas primero
-- Luego de crear los índices idx_deliveries_scheduled, idx_trips_tripid y idx_trips_route, el plan cambia a Index Scan y Nested Loop, reduciendo el tiempo de 47 ms a 8 ms (ambas aproximadamente con un 84% de mejora). 
-- El índice temporal sobre deliveries es el principal responsable de la optimización.

-- QUERY 5: Conductores activos y carga de trabajo
explain analyze
select
d.driver_id,
d.first_name,
d.last_name,
count(t.trip_id) as total_trips   -- cuántos viajes hizo
from drivers d
left join trips t on d.driver_id = t.driver_id  -- LEFT JOIN porque quiero ver conductores incluso si no hicieron viajes
where d.status = 'active'   -- solo conductores activos
group by d.driver_id, d.first_name, d.last_name   -- agrupo por conductor
order by total_trips desc; -- los que más viajaron arriba
-- La Query 5 mejoró tras crear el índice idx_trips_driver, pasando de Hash Join a Index Scan y reduciendo el tiempo de ejecución muy significativamente.

-- QUERY 6: Promedio de entregas por conductor (6 meses)
explain analyze
select
d.driver_id,
d.first_name,
d.last_name,
count(del.delivery_id) as total_deliveries_6m,  -- cuántas entregas hizo en 6 meses
round(count(del.delivery_id)::numeric / 6, 2) as avg_per_month   -- promedio mensual = entregas_totales / 6 meses
from drivers d
join trips t on t.driver_id = d.driver_id   -- via trips consigo qué viajes pertenecen a cada conductor
join deliveries del on del.trip_id = t.trip_id   -- via deliveries consigo qué entregas pertenecen a cada viaje
where del.scheduled_datetime >= current_date - interval '6 months'  -- filtro periodo de análisis: 6 meses
group by d.driver_id, d.first_name, d.last_name  
order by total_deliveries_6m desc;   -- el más productivo arriba
-- Depende de filtros temporales y múltiples JOIN.
-- Con índices sobre scheduled_datetime, driver_id y trip_id, el plan cambió a Index Scan con Nested Loops,
-- reduciendo significativamente el tiempo.

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

-- QUERY 10: Ranking de conductores por eficiencia
explain analyze
select d.driver_id, d.first_name, d.last_name,
count(t.trip_id) AS total_trips,    -- cantidad de viajes
rank() over(order by count(t.trip_id) desc) as efficiency_rank  -- WINDOW FUNCTION: calcula ranking ordenado por viajes
from drivers d
join trips t on t.driver_id = d.driver_id 
group by d.driver_id, d.first_name, d.last_name  -- necesarios para COUNT + RANK
order by efficiency_rank   -- ranking final
limit 20;       -- top 20 conductores
-- La creación del índice idx_trips_driver permitió una mejora muy significativa en el tiempo de ejecución.

