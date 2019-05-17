import boto3
from boto3.session import Session
from boto3.dynamodb.conditions import Key, Attr
import os
import pickle
from io import BytesIO
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder, LabelBinarizer,MultiLabelBinarizer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.linear_model import LogisticRegression
from sklearn import svm
from sklearn.model_selection import cross_val_score,StratifiedKFold, cross_validate
from sklearn.metrics import classification_report, f1_score, accuracy_score, precision_score, recall_score, roc_auc_score
from sklearn.decomposition import PCA
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import DynamoDBClient

dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb', region_name='eu-west-2')
s3 = boto3.resource('s3')

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_DEFAULT_REGION  = os.environ['AWS_DEFAULT_REGION']


class ML():

    def __init__(self):

        with BytesIO() as data:
            s3.Bucket("artificial-demo").download_fileobj("xgb.pkl", data)
            data.seek(0)    # move back to the beginning after writing
            self.xgb = pickle.load(data)
        
        db = DynamoDBClient.DynamoDB(db_name = "customer")
        self.data = pd.DataFrame(db.get_all_items())

        print("----------------------------------------")
        print(self.data.tail(1))
        
    def predict(self):


        y_pred=self.xgb.predict([self.X[-1]])
        y_proba=self.xgb.predict_proba([self.X[-1]])
        print(y_pred)
        print(y_proba)

        return y_pred,y_proba
        
    def pre_process(self, data):
        print("#######  PRE-PROCESSING ###########")
        #Numeric Features
        numeric_features= list(data.columns[data.dtypes == 'int64'])
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(data[numeric_features])
        scaled_data = pd.DataFrame(data=scaled_data, columns=numeric_features)

        #Binary Features
        binary_features = ["default", "housing", "loan", "y"]
        lb = BinaryTransformer()
        binarised_features = lb.fit_transform(data[binary_features])

        # Multioutput Features
        categorical_features = list(set(list(data.columns[data.dtypes == 'object'])) - set(binarised_features))
        ohe_data = pd.get_dummies(data[categorical_features])
        new_categorical_features = ohe_data.columns

        cleaned_data = pd.concat([scaled_data, binarised_features, ohe_data], axis=1)

        
        self.X = cleaned_data.drop('y', axis=1)
        self.y = cleaned_data['y']
        
        print("####### REEEE ###########")
        print(self.X.shape)

        pca = PCA(n_components=32)
        self.X = pca.fit_transform(self.X)
        print(self.X.shape)

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(self.X, self.y, test_size=0.2, stratify=self.y)



class BinaryTransformer( BaseEstimator, TransformerMixin ):
    #Class Constructor 
    def __init__(self):
        pass
    
    #Return self nothing else to do here    
    def fit( self, X, y = None ):
        return self 
    
    #Method that describes what we need this transformer to do
    def transform( self, X, y = None ):
        self.columns = list(X.columns)
        result = X.copy()
        for c in result.columns:
            result[c] = result[c].apply(lambda x: 1 if x=="yes" else 0)
            
        return result