from flask import Flask, flash, request, redirect, url_for,render_template
import DynamoDBClient
from werkzeug.utils import secure_filename
import boto3
import os
import csv
import io
import pandas as pd
import threading
import machine_learning
from pandas.io.json import json_normalize
import numpy as np
app = Flask(__name__)


ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_DEFAULT_REGION  = os.environ['AWS_DEFAULT_REGION']

@app.route('/')
def api_root():
    string = '''
    Welcome to this demo \n
    /upload_csv to upload the bank-full to DynamoDB - NoSQL
    /predict to make a prediction
    '''

    return string


@app.route('/predict', methods=['GET','POST'])
def predict():
    ''' 
    curl --header "Content-Type: application/json" --request POST --data '{"age":"35","job":"management","martial":"married","education":"secondary","default":"no","balance":"2143","housing":"yes","loan":"no","contact":"unknown","day":"5","month":"may","duration":"261","campaign":"1","pdays":"-1","previous":"0","poutcome":"unknown"}' http://localhost:5000/predict
    
    '''


    if request.method == 'POST':
        content = request.json
        new_record = json_normalize(content)
        
        #Assign correct dtypes to the new row we just recieved
        num_col= ["age","balance","day","duration","campaign","pdays","previous"]
        new_record[num_col] = new_record[num_col].apply(pd.to_numeric)
        new_record["y"] = "no"

        xgb = machine_learning.ML()
        old_dataset = xgb.data.drop(["id"],axis=1)

        #append the latest record to the dataset we already have for pre-processing
        combined_dataset= old_dataset.append(new_record).reset_index()
        

        xgb.pre_process(combined_dataset)
        y_pred, y_proba = xgb.predict()

        y_pred= "No" if y_pred[0] == 0 else "Yes"
        y_proba = np.round(float(y_proba[0][1]), decimals=2)

        return "Prediction: {}, Probability {}".format(y_pred, y_proba)
    
    
    xgb = machine_learning.ML()


    return str(xgb.data)



@app.route('/upload_csv', methods=['GET','POST'])
def upload_csv():


    if request.method == 'GET':
        return render_template("upload_csv.html", name = "upload_csv")

    if request.method == 'POST':
        
        # check if the post request has the file part
        if 'file' not in request.files:
            return render_template("upload_csv.html", name = "upload_csv")
            #return "Please POST a file"

        file = request.files['file']

        str_file = str(file.read(), 'utf-8')
        f = io.StringIO(str_file)

        df = pd.read_csv(f, sep=";")
        print(df.head())

        json_data = df.to_json(orient='records')
        print("################")



        dynamoDBClient = DynamoDBClient.DynamoDB(db_name="customers2")

        # thread = threading.Thread(target=dynamoDBClient.convert_pd_to_json_list(df))
        # thread.start()
        dynamoDBClient.emptyTable()
        dynamoDBClient.convert_pd_to_json_list(df)
        dynamoDBClient.batch_write()


        return "file has been uploaded to DynamoDB"
