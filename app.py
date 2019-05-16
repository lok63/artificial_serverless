from flask import Flask, flash, request, redirect, url_for,render_template
import DynamoDBClient
from werkzeug.utils import secure_filename
import boto3
import os
import csv
import io
import pandas as pd
import threading

app = Flask(__name__)


ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_DEFAULT_REGION  = os.environ['AWS_DEFAULT_REGION']

@app.route('/')
def api_root():
    return 'Welcome'

@app.route('/upload_csv', methods=['GET','POST'])
def upload_csv():
    ''' curl -F ‘data=data/bank-full.csv’ 127.0.0.1:5000/upload_csv 
        curl -F ‘data=@data/bank-full.csv’ 127.0.0.1:5000/upload_csv 
        curl -i -X POST -F "files=@$data/bank-full.csv" 127.0.0.1:5000/upload_csv 
        curl -d "data=bankFull" 127.0.0.1:5000/upload_csv 
        curl --request POST --data-binary "@bankFull.csv" $127.0.0.1:5000/upload_csv
    '''

    if request.method == 'GET':
        return render_template("upload_csv.html", name = "upload_csv")

    if request.method == 'POST':
        
        # check if the post request has the file part
        if 'file' not in request.files:
            return render_template("upload_csv.html", name = "upload_csv")
            #return "Please POST a file"

        file = request.files['file']


        # print(type(str(file.read(), 'utf-8')))
        str_file = str(file.read(), 'utf-8')
        f = io.StringIO(str_file)

        df = pd.read_csv(f, sep=";")
        print(df.head())

        json_data = df.to_json(orient='records')
        print("################")



        dynamoDBClient = DynamoDBClient.DynamoDB()

        thread = threading.Thread(target=dynamoDBClient.convert_pd_to_json_list(df))
        thread.start()

        dynamoDBClient.batch_write()


        return render_template("upload_csv.html", name = "upload_csv")



        # if file:
        #     try:
                

        #         # s3 = boto3.resource('s3')
        #         # s3.meta.client.upload_file(file.name, 'artificial-demo', 'data/bank-full.csv')
                
        #         print("################")
        #         print("Upload Completed")
        #     except Exception as e:
        #         print(e)