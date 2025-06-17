from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import boto3
import uuid

def lambda_handler(event=None, context=None):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Inicializar el navegador
    driver = webdriver.Chrome(options=chrome_options)
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    driver.get(url)

    # Esperar a que cargue la tabla (puedes ajustar el tiempo)
    time.sleep(5)

    # Obtener el HTML después de que la tabla esté visible
    html = driver.page_source
    driver.quit()

    # Parsear con BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    headers = [th.text.strip() for th in table.find_all('th')]
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir encabezado
        cells = row.find_all('td')
        if len(cells) == len(headers):
            rows.append({
                headers[i]: cell.text.strip() for i, cell in enumerate(cells)
            })

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping-Sismos')

    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={'id': item['id']})

    for i, row in enumerate(rows, start=1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        table.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }
