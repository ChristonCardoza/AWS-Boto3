import boto3
import random

def lambda_handler(event, context):
    client = boto3.client('dynamodb')

    table_name = 'Employee_Details'
    # create_table_response = Create_Table(client,table_name)
    # list_table_response = List_Table(client)
    # delete_table_response = Delete_Table(client,'Test')
    # put_item_response = Put_Item(client, table_name)
    get_iem_response = Get_Item(client, table_name)

def Create_Table(dynamo,tbName):
    create = dynamo.create_table(

        TableName=tbName,
        KeySchema=[
            {
                'AttributeName': 'Designation',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Name',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Designation',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        }
    )

    print(create)
    return create

def List_Table(dynamo):
    table_list = dynamo.list_tables()
    print(table_list['TableNames'])
    return table_list['TableNames']

def Delete_Table(dynamo,tbName):
    response = dynamo.delete_table(TableName= tbName)
    print(response)
    return(response)

def Put_Item(dynamo,tbName):
    for repeat in range(10):
        Designation = ['Software Engineer','Software Quality Engineer', 'Security Analyst']
        Name = ['Christon', 'Wilfred', 'Rohan']
        SerName = ['Cardoza', 'Fernendes','D\'Souza']
        response = dynamo.put_item(
    
            TableName= tbName,
            Item={
                'Designation':{'S': random.choice(Designation)} ,
                'Name':{'S':random.choice(Name)},
                'SerName':{'S':random.choice(SerName)}
    
    
            }
        )
    print(response)
    return(response)

def Get_Item(dynamo,tbName):
    response = dynamo.get_item(

        TableName = tbName,

        Key={
            'Designation': {
                'S': 'Software Engineer',
            },
            'Name': {
                'S': 'Christon',
            }
        },

    )

    print(response['Item'])
    print(response)
