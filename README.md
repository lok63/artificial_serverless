# Sererless Application Using AWS Lambda, DynamoDB and flask


## Prediction
You can use the following example to make prediction:
**Locally** :     curl --header "Content-Type: application/json" --request POST --data '{"age":"35","job":"management","martial":"married","education":"secondary","default":"no","balance":"2143","housing":"yes","loan":"no","contact":"unknown","day":"5","month":"may","duration":"261","campaign":"1","pdays":"-1","previous":"0","poutcome":"unknown"}' http://localhost:5000/predict


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
You can use the following example to make prediction:
**Locally** :     curl --header "Content-Type: application/json" --request POST --data '{"age":"35","job":"management","martial":"married","education":"secondary","default":"no","balance":"2143","housing":"yes","loan":"no","contact":"unknown","day":"5","month":"may","duration":"261","campaign":"1","pdays":"-1","previous":"0","poutcome":"unknown"}' http://localhost:5000/predict
