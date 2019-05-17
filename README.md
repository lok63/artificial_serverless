# Sererless Application Using AWS Lambda, DynamoDB, S3 and flask

## How to run usign the AWS service

#### Upload a csv file to DynamoDB
**Note** : This process won't work on the server since the policy of Amazon API Gateway has a limnit of 30seconds and the POST re. However this process works when you run it locally. I could solve this issue by uploading the csv file on S3 first and then run the butch write command in the background. 

<del>https://fmj5rqsbf0.execute-api.eu-west-2.amazonaws.com/dev/upload_csv</del>

#### Prediction
Open your terminal and type the following command:

    curl --header "Content-Type: application/json" --request POST --data '{"age":"35","job":"management","martial":"married","education":"secondary","default":"no","balance":"2143","housing":"yes","loan":"no","contact":"unknown","day":"5","month":"may","duration":"261","campaign":"1","pdays":"-1","previous":"0","poutcome":"unknown"}' https://fmj5rqsbf0.execute-api.eu-west-2.amazonaws.com/dev/predict


## How to run locally

Create and actibate a virtual environment. You can use conda or virtualenv. Make sure you are using python3.
    
Install dependencies

    pip install -r requirements.txt
    
Run the application:

  Navigate to the root directory
  
      cd artificial_serverless/
      
  And run 
  **For Unix based systems**
  
        export flask_app=app.py
        flask run
        
  **For windows**
  
        set flask_app=app.py 
        flask run

#### Upload a csv file to DynamoDB
To access the application go to http://127.0.0.1:5000/upload_csv

#### Prediction
    curl --header "Content-Type: application/json" --request POST --data '{"age":"35","job":"management","martial":"married","education":"secondary","default":"no","balance":"2143","housing":"yes","loan":"no","contact":"unknown","day":"5","month":"may","duration":"261","campaign":"1","pdays":"-1","previous":"0","poutcome":"unknown"}' http://localhost:5000/predict
