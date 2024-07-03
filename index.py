import boto3
#Configuracao Boto3 para usar com o localstack
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:9000',  #Localstack roda local
                          region_name='us-west-2',  
                          aws_access_key_id='test',  
                          aws_secret_access_key='test')  

# Criação da tabela
def create_table():
    table = dynamodb.create_table(
        TableName='fiap',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    # Aqui ele espera a tabela ser criada se não existir
    table.wait_until_exists()
    print("Table created successfully.")

# Insere um item na tabela
def create_item():
    table = dynamodb.Table('fiap')
    table.put_item(
       Item={
            'id': '001',
            'name': 'Guilherme Tavares',
            'age': 26
        }
    )
    print("Item created successfully.")

# Lê um item da tabela
def read_item():
    table = dynamodb.Table('fiap')
    response = table.get_item(
        Key={
            'id': '001'
        }
    )
    item = response.get('Item', {})
    print(f"Read item: {item}")

# Atualiza um item da tabela
def update_item():
    table = dynamodb.Table('fiap')
    table.update_item(
        Key={
            'id': '001'
        },
        UpdateExpression='SET age = :val1',
        ExpressionAttributeValues={
            ':val1': 25
        }
    )
    print("Item updated successfully.")

# Deleta um item da tabela
def delete_item():
    table = dynamodb.Table('fiap')
    table.delete_item(
        Key={
            'id': '001'
        }
    )
    print("Item deleted successfully.")

if __name__ == "__main__":
    create_table()
    create_item()
    read_item()
    update_item()
    read_item()
    delete_item()