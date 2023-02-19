import json
from pprint import pprint
from re import sub
from textwrap import indent
from typing import Protocol
import boto3
import pandas as pd

AWS_KEY_ID = 'AKIASCJYQIUOXFKJOQPP'
AWS_SECRET_KEY = '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'

# build up sns
sns = boto3.client(
    'sns',
    region_name='us-east-1',
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_KEY
)

trash_arn = sns.create_topic(Name="trash_notifs")['TopicArn']
streets_arn = sns.create_topic(Name="street_notifs")['TopicArn']

# subscribe users
contacts = pd.read_csv('50-contacts.csv')

def subscribe_user(user_row):
    if user_row['department'] == 'trash':
        sns.subscribe(TopicArn = trash_arn, Protocol='sms', Endpoint=str(user_row['phone']))
        sns.subscribe(TopicArn = trash_arn, Protocol='email', Endpoint=str(user_row['email']))
    else:
        sns.subscribe(TopicArn = streets_arn, Protocol='sms', Endpoint=str(user_row['phone']))
        sns.subscribe(TopicArn = streets_arn, Protocol='email', Endpoint=str(user_row['email']))

contacts.apply(subscribe_user, axis=1)

dept_arns = {} 

for dept in contacts['department']:
  critical = sns.create_topic(Name="{}_critical".format(dept))
  extreme = sns.create_topic(Name="{}_extreme".format(dept))
  dept_arns['{}_critical'.format(dept)] = critical['TopicArn']
  dept_arns['{}_extreme'.format(dept)] = extreme['TopicArn']

pprint(dept_arns)


for index, user_row in contacts.iterrows():
  critical_tname = '{}_critical'.format(user_row['department'])
  extreme_tname = '{}_extreme'.format(user_row['department'])
  
  critical_arn = sns.create_topic(Name=critical_tname)['TopicArn']
  extreme_arn = sns.create_topic(Name=extreme_tname)['TopicArn']
  

  sns.subscribe(TopicArn = critical_arn, 
                Protocol='email', Endpoint=user_row['email'])
  sns.subscribe(TopicArn = extreme_arn, 
                Protocol='sms', Endpoint=str(user_row['phone']))


# vcounts = {}
# 
# if vcounts['water'] > 100:
#   # If over 100 water violations, publish to water_critical
#   sns.publish(
#     TopicArn = dept_arns['water_critical'],
#     Message = "{} water issues".format(vcounts['water']),
#     Subject = "Help fix water violations NOW!")

# if vcounts['water'] > 300:
#   # If over 300 violations, publish to water_extreme
#   sns.publish(
#     TopicArn = dept_arns['water_extreme'],
#     Message = "{} violations! RUN!".format(vcounts['water']),
#     Subject = "THIS IS BAD.  WE ARE FLOODING!")