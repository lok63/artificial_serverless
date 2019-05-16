import csv
import boto3
from boto3.session import Session
from boto3.dynamodb.conditions import Key, Attr
import os
dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb', region_name='eu-west-2')


ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_DEFAULT_REGION  = os.environ['AWS_DEFAULT_REGION']

class DynamoDB():

    def __init__(self, db_name):
        self.db_name = db_name
        self.existing_tables = client.list_tables()['TableNames']

        


    def pd_to_json(self, df):
        self.json_data = df.to_json(orient='records')

    def convert_pd_to_json_list(self,df):
        items = []    
        counter = 1
        
        for index, row in df.iterrows():
            data = {}
            data['id'] = counter
            data['age'] = row['age']
            data['job'] = row['job']
            data['marital'] = row['marital']
            data['education'] = row['education']
            data['default'] = row['default']
            data['balance'] = row['balance']
            data['housing'] = row['housing']
            data['loan'] = row['loan']
            data['contact'] = row['contact']
            data['day'] = row['day']
            data['month'] = row['month']
            data['duration'] = row['duration']
            data['campaign'] = row['campaign']
            data['pdays'] = row['pdays']
            data['previous'] = row['previous']
            data['poutcome'] = row['poutcome']
            data['y'] = row['y']

            items.append(data)
            counter +=1 


        self.json_data= items
        print("################")
        print("Start uploading")

    def batch_write(self):
        items = self.json_data
        boto3.dynamodb_session = Session(aws_access_key_id=ACCESS_KEY,
                                            aws_secret_access_key=SECRET_KEY,
                                            region_name=AWS_DEFAULT_REGION)

        dynamodb = boto3.resource('dynamodb')
        db = dynamodb.Table(self.db_name)
        

        with db.batch_writer() as batch:
            print("################")
            print("Start uploading")
            for item in items:
                batch.put_item(Item=item)


    def deleteTable(self):
        if self.db_name in self.existing_tables:
            print('deleting table')
            return client.delete_table(TableName=self.db_name)


    def createTable(self):
        print("################")
        print("Creating new table")

        # waiter = client.get_waiter('table_not_exists')
        # waiter.wait(TableName=self.db_name)


        if self.db_name not in self.existing_tables:
            print('creating table')

            response  = client.create_table(
                TableName=self.db_name,
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions= [],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 100
                },
                StreamSpecification={
                    'StreamEnabled': False
                }
            )


    def emptyTable(self):
        self.deleteTable()
        self.createTable()



