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

-- (Opcional) Para conductores activos (Query 5)
CREATE INDEX idx_drivers_status_active 
ON drivers(status)
WHERE status = 'active';