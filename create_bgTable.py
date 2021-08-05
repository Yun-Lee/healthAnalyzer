# -*- coding: utf-8 -*-
"""
Created on Wed Aug  4 17:09:06 2021

@author: YunL
"""

import pandas as pd
import numpy as np
import os
from google.cloud import bigquery as bq
from dateutil.relativedelta import relativedelta
from datetime import datetime
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_info(
    {
    "type": "service_account",
    "project_id": "groupName",
    "private_key_id": "private_key_id",
    "private_key": "一串巨長的private_key",
    "client_email": "client_email",
    "client_id": "client_id",
    "auth_uri": "auth_uri",
    "token_uri": "tiken_uri",
    "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
    "client_x509_cert_url": "client_x509_cert_url"
    }
    )

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="D:/jubo-ai-e6e0deefa60f.json"
client = bq.Client()

#vitalsigns table
dataset_id= f"{client.project}.health_dashboard"
table_id1= f"{dataset_id}.vitalsigns"
tschema1= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("TP", "FLOAT", mode="NULLABLE"),
bq.SchemaField("PR", "FLOAT", mode="NULLABLE"),
bq.SchemaField("RR", "FLOAT", mode="NULLABLE"),
bq.SchemaField("SYS", "FLOAT", mode="NULLABLE"),
bq.SchemaField("DIA", "FLOAT", mode="NULLABLE"),
bq.SchemaField("SPO2", "FLOAT", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table1= bq.Table(table_id1, schema=tschema1)
table1= client.create_table(table1)


#bloodsugar table
table_id2= f"{dataset_id}.bloodsugar_insulin"
tschema2= [
bq.SchemaField("bloodsugar_id", "STRING", mode="NULLABLE"),
bq.SchemaField("sugarType", "DATETIME", mode="NULLABLE"),
bq.SchemaField("sugarValue", "STRING", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate_bs", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("insulin_id", "STRING", mode="NULLABLE"),
bq.SchemaField("medicine", "STRING", mode="NULLABLE"),
bq.SchemaField("dose", "STRING", mode="NULLABLE"),
bq.SchemaField("part", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate_insulin", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("state", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table2= bq.Table(table_id2, schema=tschema2)
table2= client.create_table(table2)


#weight table
table_id3= f"{dataset_id}.weight"
tschema3= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("height", "FLOAT", mode="NULLABLE"),
bq.SchemaField("weight", "FLOAT", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("bmi", "FLOAT", mode="NULLABLE"),
bq.SchemaField("difference", "FLOAT", mode="NULLABLE"),
bq.SchemaField("differenceWeight", "FLOAT", mode="NULLABLE"),
bq.SchemaField("month3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("monthWeight3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("month6", "FLOAT", mode="NULLABLE"),
bq.SchemaField("monthWeight6", "FLOAT", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table3= bq.Table(table_id3, schema=tschema3)
table3= client.create_table(table3)


#nutritionrecord table
table_id4= f"{dataset_id}.nutritionrecords"
tschema4= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("mobility", "STRING", mode="NULLABLE"),
bq.SchemaField("foodType", "STRING", mode="NULLABLE"),
bq.SchemaField("foodIntake", "STRING", mode="NULLABLE"),
bq.SchemaField("nutrientIntake", "STRING", mode="NULLABLE"),
bq.SchemaField("advice", "STRING", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("feedingMethod", "STRING", mode="NULLABLE"),
bq.SchemaField("feedingAbility", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table4= bq.Table(table_id4, schema=tschema4)
table4= client.create_table(table4)


#mininutritions table
table_id5= f"{dataset_id}.mininutritions"
tschema5= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("Q11", "LIST", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("Q1", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q2", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q3", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q4", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q5", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q6", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q7", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q9", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q10", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q8", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q12", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q13", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q14", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q15", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q16", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q17", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q18", "INTEGER", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table5= bq.Table(table_id5, schema=tschema5)
table5= client.create_table(table5)


#assessment table
table_id6= f"{dataset_id}.assessment"
tschema6= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("Q11", "LIST", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("Q1", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q2", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q3", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q4", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q5", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q6", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q7", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q9", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q10", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q8", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q12", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q13", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q14", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q15", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q16", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q17", "INTEGER", mode="NULLABLE"),
bq.SchemaField("Q18", "INTEGER", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table6= bq.Table(table_id6, schema=tschema6)
table6= client.create_table(table6)


#mmse table
table_id7= f"{dataset_id}.mmses"
tschema7= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("education", "STRING", mode="NULLABLE"),
bq.SchemaField("orientationQ1", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ2", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ4", "FLOAT", mode="NULLABLE"),
bq.SchemaField("constructiveQ1", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ6", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ7", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ8", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ4", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ5", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ2", "FLOAT", mode="NULLABLE"),
bq.SchemaField("languageQ1", "FLOAT", mode="NULLABLE"),
bq.SchemaField("memoryQ3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("memoryQ2", "FLOAT", mode="NULLABLE"),
bq.SchemaField("memoryQ1", "FLOAT", mode="NULLABLE"),
bq.SchemaField("attentionAndCalcQ1", "FLOAT", mode="NULLABLE"),
bq.SchemaField("attentionAndCalcQ2", "FLOAT", mode="NULLABLE"),
bq.SchemaField("attentionAndCalcQ3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("attentionAndCalcQ4", "FLOAT", mode="NULLABLE"),
bq.SchemaField("attentionAndCalcQ5", "FLOAT", mode="NULLABLE"),
bq.SchemaField("memoryRecordQ1", "FLOAT", mode="NULLABLE"),
bq.SchemaField("memoryRecordQ2", "FLOAT", mode="NULLABLE"),
bq.SchemaField("memoryRecordQ3", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ10", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ9", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ8", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ7", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ6", "FLOAT", mode="NULLABLE"),
bq.SchemaField("orientationQ5", "FLOAT", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("unassessable", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table7= bq.Table(table_id7, schema=tschema7)
table7= client.create_table(table7)


#phyassts table
table_id8= f"{dataset_id}.phyassts"
tschema8= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("conscious", "STRING", mode="NULLABLE"),
bq.SchemaField("activity", "STRING", mode="NULLABLE"),
bq.SchemaField("constraints", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("intake", "STRING", mode="NULLABLE"),
bq.SchemaField("musUpperL", "STRING", mode="NULLABLE"),
bq.SchemaField("musUpperR", "STRING", mode="NULLABLE"),
bq.SchemaField("musLowerL", "STRING", mode="NULLABLE"),
bq.SchemaField("musLowerR", "STRING", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("musTypeUpperR", "STRING", mode="NULLABLE"),
bq.SchemaField("musTypeUpperL", "STRING", mode="NULLABLE"),
bq.SchemaField("musTypeLowerR", "STRING", mode="NULLABLE"),
bq.SchemaField("musTypeLowerL", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table8= bq.Table(table_id8, schema=tschema8)
table8= client.create_table(table8)


#bedsore table
table_id9= f"{dataset_id}.bedsores"
tschema9= [
bq.SchemaField("finished", "STRING", mode="NULLABLE"),
bq.SchemaField("woundType", "STRING", mode="NULLABLE"),
bq.SchemaField("locationOccur", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate_bedsore", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("site", "STRING", mode="NULLABLE"),
bq.SchemaField("grade", "STRING", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("locationOccurMemo", "STRING", mode="NULLABLE"),
bq.SchemaField("latestGrade", "STRING", mode="NULLABLE"),
bq.SchemaField("finishedNote", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate_detail", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("skinStatus", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table9= bq.Table(table_id9, schema=tschema9)
table9= client.create_table(table9)


#marv2 table
table_id10= f"{dataset_id}.marv2"
tschema10= [
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("room", "STRING", mode="NULLABLE"),
bq.SchemaField("bed", "STRING", mode="NULLABLE"),
bq.SchemaField("birthday", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("checkInDate", "STRING", mode="NULLABLE"),
bq.SchemaField("sex", "STRING", mode="NULLABLE"),
bq.SchemaField("idNumber", "STRING", mode="NULLABLE"),
bq.SchemaField("familyHealthProblems", "STRING", mode="NULLABLE"),
bq.SchemaField("medications", "STRING", mode="NULLABLE"),
bq.SchemaField("medicalHistory", "STRING", mode="NULLABLE"),
bq.SchemaField("status", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("tube", "STRING", mode="NULLABLE"),
bq.SchemaField("drugAllergies", "STRING", mode="NULLABLE"),
bq.SchemaField("foodAllergies", "STRING", mode="NULLABLE"),
bq.SchemaField("branch", "STRING", mode="NULLABLE"),
bq.SchemaField("emgcyCont", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("emgcyContPhone", "STRING", mode="NULLABLE"),
bq.SchemaField("emgcyContRelation", "STRING", mode="NULLABLE"),
bq.SchemaField("doctorDivision", "STRING", mode="NULLABLE"),
bq.SchemaField("hospital", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("DNRConsent", "STRING", mode="NULLABLE"),
bq.SchemaField("hospital", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("emgcyContAddress", "STRING", mode="NULLABLE"),
bq.SchemaField("phone", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table10= bq.Table(table_id10, schema=tschema10)
table10= client.create_table(table10)


#smartschedule table
table_id11= f"{dataset_id}.smartschedule"
tschema11= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("completed", "BOOLEAN", mode="NULLABLE"),
bq.SchemaField("deleted", "BOOLEAN", mode="NULLABLE"),
bq.SchemaField("task", "STRING", mode="NULLABLE"),
bq.SchemaField("startTime", "DATETIME", mode="NULLABLE"),
bq.SchemaField("endTime", "DATETIME", mode="NULLABLE"),
bq.SchemaField("category", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "STRING", mode="NULLABLE"),
bq.SchemaField("subCategory", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table11= bq.Table(table_id11, schema=tschema11)
table11= client.create_table(table11)


#shift table
table_id12= f"{dataset_id}.shifts"
tschema12= [
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("shiftname", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "TIMESTAMP", mode="NULLABLE"),
bq.SchemaField("notes", "STRING", mode="NULLABLE"),
bq.SchemaField("notification", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table12= bq.Table(table_id12, schema=tschema12)
table12= client.create_table(table12)


#nursingrecords table
table_id13= f"{dataset_id}.nursingrecords"
tschema13= [
bq.SchemaField("_id", "STRING", mode="NULLABLE"),
bq.SchemaField("nursingDiagnosis", "STRING", mode="NULLABLE"),
bq.SchemaField("patient", "STRING", mode="NULLABLE"),
bq.SchemaField("createdDate", "Timestamp", mode="NULLABLE"),
bq.SchemaField("evaluation", "STRING", mode="NULLABLE"),
bq.SchemaField("goals", "STRING", mode="NULLABLE"),
bq.SchemaField("changePlan", "STRING", mode="NULLABLE"),
bq.SchemaField("organization", "STRING", mode="NULLABLE"),
bq.SchemaField("name", "STRING", mode="NULLABLE"),
bq.SchemaField("org_name", "STRING", mode="NULLABLE"),
bq.SchemaField("nickName", "STRING", mode="NULLABLE"),
bq.SchemaField("solution", "STRING", mode="NULLABLE")
]
table13= bq.Table(table_id13, schema=tschema13)
table13= client.create_table(table13)


'''
allPA=pd.read_csv("D:/allPA0506.csv", index_col=0)
allPA["occurred_at"]= pd.to_datetime(allPA["occurred_at"])
allPA["occurred_date"]= pd.to_datetime(allPA["occurred_date"])
allPA["occurred_time"]= pd.to_datetime(allPA["occurred_time"])

awayPA=pd.read_csv("D:/away-from-bed0506.csv", index_col=0)
awayPA["occurred_at"]= pd.to_datetime(awayPA["occurred_at"])
awayPA["occurred_date"]= pd.to_datetime(awayPA["occurred_date"])
awayPA["occurred_time"]= pd.to_datetime(awayPA["occurred_time"])

list=pd.read_csv("D:/fall0506.csv", index_col=0)
list["occurred_at"]= pd.to_datetime(list["occurred_at"])
list["occurred_date"]= pd.to_datetime(list["occurred_date"])
list["occurred_time"]= pd.to_datetime(list["occurred_time"])

allvs=pd.read_csv("D:/allvs0506.csv", index_col=0)
allvs["occurred_at"]= pd.to_datetime(allvs["occurred_at"])
allvs["occurred_date"]= pd.to_datetime(allvs["occurred_date"])
allvs["occurred_time"]= pd.to_datetime(allvs["occurred_time"])

allPA.to_gbq(destination_table = 'Jubo_space.PA', project_id="jubo-ai", if_exists = 'replace')
awayPA.to_gbq(destination_table = 'Jubo_space.away_from_bed', project_id="jubo-ai", if_exists = 'replace')
list.to_gbq(destination_table = 'Jubo_space.fall_list', project_id="jubo-ai", if_exists = 'replace')
allvs.to_gbq(destination_table = 'Jubo_space.VS', project_id="jubo-ai", if_exists = 'replace')
'''
