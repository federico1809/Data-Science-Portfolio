"""
FleetLogix - Funciones Lambda
Lógica de negocio serverless para procesamiento de eventos logísticos.
"""

import json
import math
from datetime import datetime, timedelta
import boto3

# Simulación de clientes AWS dentro de Lambda
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Constantes de Negocio
DEVIATION_THRESHOLD_KM = 2.0  # Umbral para alerta de desvío
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789:FleetLogixAlerts"

# =====================================================
# LAMBDA 1: Verificar si una entrega se completó
# Trigger: API Gateway (GET /delivery/{id}/status)
# =====================================================
def lambda_verificar_entrega(event, context):
    """
    Consulta DynamoDB para verificar el estado de una entrega en tiempo real.
    """
    delivery_id = event.get('delivery_id')
    
    table = dynamodb.Table('FleetLogix_Deliveries_Status')
    
    try:
        response = table.get_item(Key={'delivery_id': int(delivery_id)})
        
        if 'Item' in response:
            status = response['Item'].get('status')
            is_completed = (status == 'Delivered')
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'delivery_id': delivery_id,
                    'is_completed': is_completed,
                    'current_status': status,
                    'timestamp': datetime.now().isoformat()
                })
            }
        else:
            return {'statusCode': 404, 'body': json.dumps('Entrega no encontrada')}
            
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(str(e))}

# =====================================================
# LAMBDA 2: Calcular Tiempo Estimado de Llegada (ETA)
# Trigger: EventBridge (Cada 5 min) o Update GPS
# =====================================================
def lambda_calcular_eta(event, context):
    """
    Calcula el ETA basado en la distancia restante y velocidad promedio.
    """
    distancia_restante_km = event.get('distance_remaining_km')
    velocidad_promedio_kmh = event.get('avg_speed_kmh', 60) # Default 60 km/h
    
    if velocidad_promedio_kmh <= 0:
        velocidad_promedio_kmh = 30 # Fallback por tráfico
        
    horas_restantes = distancia_restante_km / velocidad_promedio_kmh
    eta = datetime.now() + timedelta(hours=horas_restantes)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'vehicle_id': event.get('vehicle_id'),
            'eta_hours': round(horas_restantes, 2),
            'estimated_arrival': eta.strftime('%Y-%m-%d %H:%M:%S')
        })
    }

# =====================================================
# LAMBDA 3: Enviar alerta si camión se desvía
# Trigger: IoT Core / Stream de GPS
# =====================================================
def calcular_distancia_haversine(lat1, lon1, lat2, lon2):
    """Auxiliar para calcular distancia entre dos puntos geográficos"""
    R = 6371  # Radio tierra km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def lambda_monitor_desvio(event, context):
    """
    Compara ubicación actual vs ruta planificada.
    Si el desvío > umbral, envía SMS/Email vía SNS.
    """
    current_lat = event['current_lat']
    current_lon = event['current_lon']
    # Simplificación: Punto más cercano de la ruta planeada
    route_lat = event['planned_lat'] 
    route_lon = event['planned_lon']
    
    distancia_desvio = calcular_distancia_haversine(current_lat, current_lon, route_lat, route_lon)
    
    is_deviated = distancia_desvio > DEVIATION_THRESHOLD_KM
    
    if is_deviated:
        mensaje = f"ALERTA: Vehículo {event['vehicle_id']} desviado por {round(distancia_desvio, 2)} km."
        
        # Enviar notificación a gerencia
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=mensaje,
            Subject="Alerta de Desvío FleetLogix"
        )
        
        # Registrar incidente en DynamoDB para auditoría
        table_alerts = dynamodb.Table('FleetLogix_Alerts')
        table_alerts.put_item(Item={
            'vehicle_id': event['vehicle_id'],
            'timestamp': datetime.now().isoformat(),
            'type': 'DEVIATION',
            'details': mensaje
        })
        
    return {
        'statusCode': 200,
        'body': json.dumps({
            'vehicle_id': event['vehicle_id'],
            'is_deviated': is_deviated,
            'deviation_km': round(distancia_desvio, 2)
        })
    }