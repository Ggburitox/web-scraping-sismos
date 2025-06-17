import requests
import boto3
import uuid
import json

def lambda_handler(event, context):
    # URL del endpoint real que devuelve datos en JSON
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

    # Realizar la solicitud HTTP a la API
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    datos = response.json()
    ultimos = datos[-10:]  # Tomamos los últimos 10 sismos
    ultimos.reverse()      # Para que estén en orden cronológico

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingPropuesto')

    # Eliminar todos los elementos actuales
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})

    # Insertar los nuevos datos
    for i, row in enumerate(ultimos, start=1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        table.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': json.dumps(ultimos)
    }
