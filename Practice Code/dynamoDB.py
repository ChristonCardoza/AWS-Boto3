import boto3
import random

def lambda_handler(event, context):
    client = boto3.client('dynamodb')

    table_name = 'Employee_Details'
    # create_table_response = Create_Table(client,table_name)
    # list_table_response = List_Table(client)
    # delete_table_response = Delete_Table(client,'Test')
    # put_item_response = Put_Item(client, table_name)
    # get_item_response = Get_Item(client, table_name)
    # delete_item_response = Delete_Item(client, table_name)
    # update_item_response = Update_Item(client, table_name)
    # query_item_response = Query_Item(client, table_name)
    scan_item_response = Scan_Item(client, table_name)


def Create_Table(dynamo,tbName):
    create = dynamo.create_table(

        TableName=tbName,
        KeySchema=[
            {
                'AttributeName': 'Name',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Surname',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Surname',
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
        Surname = ['Cardoza', 'Fernendes','D\'Souza']
        response = dynamo.put_item(

            TableName= tbName,
            Item={

                'Name':{'S':random.choice(Name)},
                'Surname':{'S':random.choice(Surname)},
                'Designation':{'S': random.choice(Designation)}


            }
        )
    print(response)
    return(response)

def Get_Item(dynamo,tbName):
    response = dynamo.get_item(

        TableName = tbName,

        Key={

            'Name': {
                'S': 'Christon'
            },
            'Surname': {
                'S': 'Cardoza'
            }

        }

    )

    print(response['Item'])
    print(response)

def Delete_Item(dynamo,tbName):
    response = dynamo.delete_item(

        TableName = tbName,

        Key={

            'Name': {
                'S': 'Christon'
            },
            'Surname': {
                'S': 'Cardoza'
            }

        }

    )

    # print(response['Item'])
    print(response)

def Update_Item(dynamo,tbName):

    response = dynamo.update_item(

        TableName = tbName,

        ExpressionAttributeNames={'#Y': 'Year_of_joining' },

        ExpressionAttributeValues={':y': {'N': '2020' }},

        Key={
            'Name': {
                'S': 'Rohan'
            },
            'Surname': {
                'S': 'Cardoza'
            }
        },

        UpdateExpression='SET #Y = :y',

        ReturnValues='UPDATED_NEW'
    )

    print(response)


def Query_Item(dynamo,tbName):

    response = dynamo.query(

        TableName=tbName,

        Select='ALL_ATTRIBUTES',

        # ProjectionExpression='Designation',

        Limit=2,

        ExpressionAttributeNames={'#N': 'Name','#Y': 'Year_of_joining'},

        ExpressionAttributeValues={':employee_name': {'S': 'Rohan' },':year': {'N': '2020' } },

        KeyConditionExpression= '#N = :employee_name',

        FilterExpression= '#Y = :year'

    )

    print(response)

def Scan_Item(dynamo,tbName):

    response = dynamo.scan(

        TableName=tbName,

        # Select='ALL_ATTRIBUTES',

        ProjectionExpression= '#N',

        Limit=2,

        # TotalSegments=2,

        # Segment=1,

        ExpressionAttributeNames={'#D': 'Designation','#N': 'Name'},

        ExpressionAttributeValues={':designation': {'S': 'Software Quality Engineer'} },

        FilterExpression= '#D = :designation'

    )

    print(response)


