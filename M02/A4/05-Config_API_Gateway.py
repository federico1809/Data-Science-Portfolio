"""
FleetLogix - Configuración de API Gateway (Extra Credits)
Script para diseñar la API REST que conecta la App Móvil con las Lambdas.
"""

import boto3
import json

# Cliente Boto3 para API Gateway
apigateway = boto3.client('apigateway', region_name='us-east-1')

# ARNs de las Lambdas (Simulados, obtenidos del deploy anterior)
LAMBDA_VERIFICAR_ARN = "arn:aws:lambda:us-east-1:123456789:function:FleetLogix_VerificarEntrega"
LAMBDA_ETA_ARN = "arn:aws:lambda:us-east-1:123456789:function:FleetLogix_CalcularETA"
LAMBDA_DESVIO_ARN = "arn:aws:lambda:us-east-1:123456789:function:FleetLogix_MonitorDesvio"

def crear_api_gateway():
    print("🌐 Creando API Gateway: FleetLogix Mobile API...")
    
    # 1. Crear la API REST
    api = apigateway.create_rest_api(
        name='FleetLogix Mobile API',
        description='API para conductores y monitoreo de flota',
        endpointConfiguration={'types': ['REGIONAL']}
    )
    api_id = api['id']
    print(f"✅ API creada con ID: {api_id}")
    
    # Obtener el recurso raíz (/)
    resources = apigateway.get_resources(restApiId=api_id)
    root_id = resources['items'][0]['id']
    
    # =================================================================
    # RUTA 1: /delivery/verify (POST) -> Lambda Verificar Entrega
    # =================================================================
    print("   ↳ Configurando recurso /delivery/verify...")
    resource_delivery = apigateway.create_resource(
        restApiId=api_id,
        parentId=root_id,
        pathPart='delivery'
    )
    resource_verify = apigateway.create_resource(
        restApiId=api_id,
        parentId=resource_delivery['id'],
        pathPart='verify'
    )
    
    # Método POST
    apigateway.put_method(
        restApiId=api_id,
        resourceId=resource_verify['id'],
        httpMethod='POST',
        authorizationType='NONE' # Abierto para demo
    )
    
    # Integración con Lambda
    apigateway.put_integration(
        restApiId=api_id,
        resourceId=resource_verify['id'],
        httpMethod='POST',
        type='AWS_PROXY', # Pasa todo el evento a Lambda
        integrationHttpMethod='POST',
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{LAMBDA_VERIFICAR_ARN}/invocations"
    )
    
    # =================================================================
    # RUTA 2: /route/eta (GET) -> Lambda Calcular ETA
    # =================================================================
    print("   ↳ Configurando recurso /route/eta...")
    resource_route = apigateway.create_resource(
        restApiId=api_id,
        parentId=root_id,
        pathPart='route'
    )
    resource_eta = apigateway.create_resource(
        restApiId=api_id,
        parentId=resource_route['id'],
        pathPart='eta'
    )
    
    apigateway.put_method(
        restApiId=api_id,
        resourceId=resource_eta['id'],
        httpMethod='GET',
        authorizationType='NONE'
    )
    
    apigateway.put_integration(
        restApiId=api_id,
        resourceId=resource_eta['id'],
        httpMethod='GET',
        type='AWS_PROXY',
        integrationHttpMethod='POST',
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{LAMBDA_ETA_ARN}/invocations"
    )

    # =================================================================
    # RUTA 3: /vehicle/location (POST) -> Lambda Monitor Desvío
    # =================================================================
    print("   ↳ Configurando recurso /vehicle/location...")
    resource_vehicle = apigateway.create_resource(
        restApiId=api_id,
        parentId=root_id,
        pathPart='vehicle'
    )
    resource_location = apigateway.create_resource(
        restApiId=api_id,
        parentId=resource_vehicle['id'],
        pathPart='location'
    )
    
    apigateway.put_method(
        restApiId=api_id,
        resourceId=resource_location['id'],
        httpMethod='POST',
        authorizationType='NONE'
    )
    
    apigateway.put_integration(
        restApiId=api_id,
        resourceId=resource_location['id'],
        httpMethod='POST',
        type='AWS_PROXY',
        integrationHttpMethod='POST',
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{LAMBDA_DESVIO_ARN}/invocations"
    )

    # =================================================================
    # DEPLOY DEL API
    # =================================================================
    print("🚀 Desplegando API en entorno 'prod'...")
    deployment = apigateway.create_deployment(
        restApiId=api_id,
        stageName='prod',
        description='Despliegue inicial FleetLogix API'
    )
    
    api_url = f"https://{api_id}.execute-api.us-east-1.amazonaws.com/prod"
    print(f"\n✅ ¡Despliegue Exitoso!\nURL Base del API: {api_url}")
    return api_url

if __name__ == '__main__':
    crear_api_gateway()