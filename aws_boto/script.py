import json
from pprint import pprint
from textwrap import indent
import boto3
import pandas as pd

AWS_KEY_ID = 'AKIASCJYQIUOXFKJOQPP'
AWS_SECRET_KEY = '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'

## 1
# Generate the boto3 client for interacting with S3 and SNS
s3 = boto3.client('s3', region_name='us-east-1', 
                         aws_access_key_id=AWS_KEY_ID, 
                         aws_secret_access_key=AWS_SECRET_KEY)

sns = boto3.client('sns', region_name='us-east-1', 
                         aws_access_key_id=AWS_KEY_ID, 
                         aws_secret_access_key=AWS_SECRET_KEY)

# List S3 buckets and SNS topics
buckets = s3.list_buckets()
topics = sns.list_topics()

# Print out the list of SNS topics
print(topics)
buckets_pretty = json.loads(buckets, indent=4)
pprint(buckets)



# ### 2
# # Create boto3 client to S3
# s3 = boto3.client('s3', region_name='us-east-1', 
#                          aws_access_key_id=AWS_KEY_ID, 
#                          aws_secret_access_key=AWS_SECRET)

# Create the buckets
# response_staging = s3.create_bucket(Bucket='gim-staging')
# response_processed = s3.create_bucket(Bucket='gim-processed')
# response_test = s3.create_bucket(Bucket='ulugbek')

# # # Print out the response
# # print(response_staging)
# buckets = s3.list_buckets()
# pprint(buckets)


### 3
# Get the list_buckets response
response = s3.list_buckets()

for bucket in response['Buckets']:
    print(bucket['Name'])



# ### 4
# # Delete the gim-test bucket
# s3.delete_bucket(Bucket='gim-test')

# # Get the list_buckets response
# response = s3.list_buckets()

# # Print each Buckets Name
# for bucket in response['Buckets']:
#     print(bucket['Name'])



### 5
# Get the list_buckets response
response = s3.list_buckets()

# Delete all the buckets with 'gim', create replacements.
for bucket in response['Buckets']:
  if 'gim' in bucket['Name']:
      s3.delete_bucket(Bucket=bucket['Name'])
    
s3.create_bucket(Bucket='gid-staging')
s3.create_bucket(Bucket='gid-processed')
  
# Print bucket listing after deletion
response = s3.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])



### 6
# Upload final_report.csv to gid-staging
s3.upload_file(Bucket='ulugbek',
               Key='2019/get_it_done.csv', 
               Filename='get_it_done.csv')

# Get object metadata and print it
response = s3.head_object(Bucket='ulugbek', 
                       Key='2019/get_it_done.csv')

# Print the size of the uploaded object
print(response['ContentLength'])



# ### 7
# # List only objects that start with '2018/final_'
# response = s3.list_objects(Bucket='gid_staging', 
#                            Prefix='2018/final_')

# # Iterate over the objects
# if 'Contents' in response:
#   for obj in response['Contents']:
#       # Delete the object
#       s3.delete_object(Bucket='gid_staging', Key=obj['Key'])

# # Print the keys of remaining objects in the bucket
# response = s3.list_objects(Bucket='gid-staging')

# for obj in response['Contents']:
#   	print(obj['Key'])



### 8
# # Upload final_report.csv to gid-staging
# s3.upload_file(Bucket='gid-staging',
#               # Set filename and key
#                Key='2019/final_report_01_01.csv', 
#                Filename='final_report.csv')

# # Get object metadata and print it
# response = s3.head_object(Bucket='gid-staging', 
#                        Key='2019/final_report_01_01.csv')

# # Print the size of the uploaded object
# print(response['ContentLength'])

## 9
# List only objects that start with '2018/final_'
response = s3.list_objects(Bucket='gid-staging', 
                           Prefix='2018/final_')

# Iterate over the objects
if 'Contents' in response:
  for obj in response['Contents']:
      # Delete the object
      s3.delete_object(Bucket='gid-staging', Key=obj['Key'])

# Print the keys of remaining objects in the bucket
response = s3.list_objects(Bucket='gid-staging')

for obj in response['Contents']:
  	print(obj['Key'])



### 9 A
# url = "https://{}.{}".format(
#     "ulugbek",
#     "2019/get_it_done.csv"
# )

# df = pd.read_csv(url)

### 10
# # Upload the final_report.csv to gid-staging bucket
# s3.upload_file(
#   # Complete the filename
#   Filename='./final_report.csv', 
#   # Set the key and bucket
#   Key='2019/final_report_2019_02_20.csv', 
#   Bucket='gid-staging',
#   # During upload, set ACL to public-read
#   ExtraArgs = {
#     'ACL': 'public-read'}
# )



## 11
# List only objects that start with '2019/final_'
response = s3.list_objects(
    Bucket='gid-staging', Prefix='2019/final_')

# Iterate over the objects
for obj in response['Contents']:

    # Give each object ACL of public-read
    s3.put_object_acl(Bucket='gid-staging', 
                      Key=obj['Key'], 
                      ACL='public-read')
    
    # Print the Public Object URL for each object
    print("https://{}.s3.amazonaws.com/{}".format('gid-staging', obj['Key']))



### 12
# # Generate presigned_url for the uploaded object
# share_url = s3.generate_presigned_url(
#   # Specify allowable operations
#   ClientMethod='get_object',
#   # Set the expiration time
#   ExpiresIn=3600,
#   # Set bucket and shareable object's name
#   Params={'Bucket': 'gid-staging','Key': 'final_report.csv'}
# )

# # Print out the presigned URL
# print(share_url)



### 13
# df_list =  [ ] 

# for file in response['Contents']:
#     # For each file in response load the object from S3
#     obj = s3.get_object(Bucket='gid-requests', Key=file['Key'])
#     # Load the object's StreamingBody with pandas
#     obj_df = pd.read_csv(obj['Body'])
#     # Append the resulting DataFrame to list
#     df_list.append(obj_df)

# # Concat all the DataFrames with pandas
# df = pd.concat(df_list)

# # Preview the resulting DataFrame
# df.head()



## 14
# Generate an HTML table with no border and selected columns
services_df.to_html('./services_no_border.html',
           # Keep specific columns only
           columns=['service_name', 'link'],
           # Set border
           border=0)

# Generate an html table with border and all columns.
services_df.to_html('./services_border_all_columns.html', 
           border=1)



### 15
# Upload the lines.html file to S3
# s3.upload_file(Filename='lines.html', 
#                # Set the bucket name
#                Bucket='datacamp-public', Key='index.html',
#                # Configure uploaded file
#                ExtraArgs = {
#                  # Set proper content type
#                  'ContentType':'text/html',
#                  # Set proper ACL
#                  'ACL': 'public-read'})

# # Print the S3 Public Object URL for the new file.
# print("http://{}.s3.amazonaws.com/{}".format('datacamp-public', 'index.html'))



## 15
df_list = [] 

# Load each object from s3
for file in request_files:
    s3_day_reqs = s3.get_object(Bucket='gid-requests', 
                                Key=file['Key'])
    # Read the DataFrame into pandas, append it to the list
    day_reqs = pd.read_csv(s3_day_reqs['Body'])
    df_list.append(day_reqs)

# Concatenate all the DataFrames in the list
all_reqs = pd.concat(df_list)

# Preview the DataFrame
all_reqs.head()



## 16
# Write agg_df to a CSV and HTML file with no border
agg_df.to_csv('./feb_final_report.csv')
agg_df.to_html('./feb_final_report.html', border=0)

# Upload the generated CSV to the gid-reports bucket
s3.upload_file(Filename='./feb_final_report.csv', 
	Key='2019/feb/final_report.html', Bucket='gid-reports',
    ExtraArgs = {'ACL': 'public-read'})

# Upload the generated HTML to the gid-reports bucket
s3.upload_file(Filename='./feb_final_report.html', 
	Key='2019/feb/final_report.html', Bucket='gid-reports',
    ExtraArgs = {'ContentType': 'text/html', 
                 'ACL': 'public-read'})



## 17
# List the gid-reports bucket objects starting with 2019/
objects_list = s3.list_objects(Bucket='gid-reports', Prefix='2019/')

# Convert the response contents to DataFrame
objects_df = pd.DataFrame(objects_list['Contents'])

# Create a column "Link" that contains Public Object URL
base_url = "http://gid-reports.s3.amazonaws.com/"
objects_df['Link'] = base_url + objects_df['Key']

# Preview the resulting DataFrame
objects_df.head()



## 18
# Write objects_df to an HTML file
objects_df.to_html('report_listing.html',
    # Set clickable links
    render_links=True,
	# Isolate the columns
    columns=['Link', 'LastModified', 'Size'])

# Overwrite index.html key by uploading the new file
s3.upload_file(
  Filename='./report_listing.html', Key='index.html', 
  Bucket='gid-reports',
  ExtraArgs = {
    'ContentType': 'text/html', 
    'ACL': 'public-read'
  })



### 19
# # Initialize boto3 client for SNS
sns = boto3.client('sns', 
                   region_name='us-east-1', 
                   aws_access_key_id=AWS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_KEY)

# Create the city_alerts topic
city_alerts = sns.create_topic(Name="city_alerts")
c_alerts_arn = response['TopicArn']

# Re-create the city_alerts topic using a oneliner
c_alerts_arn_1 = sns.create_topic(Name='city_alerts')['TopicArn']

# Compare the two to make sure they match
print(c_alerts_arn == c_alerts_arn_1)



### 20 
# # Create list of departments
# departments = ['trash', 'streets', 'water']

# for dept in departments:
#   	# For every department, create a general topic
#     sns.create_topic(Name="{}_general".format(dept))
    
#     # For every department, create a critical topic
#     sns.create_topic(Name="{}_critical".format(dept))

# # Print all the topics in SNS
# response = sns.list_topics()
# pprint(response['Topics'])



### 21
# # Get the current list of topics
# topics = sns.list_topics()['Topics']

# for topic in topics:
#   # For each topic, if it is not marked critical, delete it
#   if "critical" not in topic['TopicArn']:
#     sns.delete_topic(TopicArn=topic['TopicArn'])
    
# # Print the list of remaining critical topics
# pprint(sns.list_topics()['Topics'])



### 22 
str_critical_arn = 'arn:aws:sns:us-east-1:142389298461:city_alerts';
# Subscribe Elena's phone number to streets_critical topic
resp_sms = sns.subscribe(
  TopicArn = str_critical_arn, 
  Protocol='sms', Endpoint="+998934792429")

# Print the SubscriptionArn
print(resp_sms['SubscriptionArn'])
print("\n")

# Subscribe Elena's email to streets_critical topic.
resp_email = sns.subscribe(
  TopicArn = str_critical_arn, 
  Protocol='email', Endpoint="u.bakhromov@student.inha.uz")

# # Print the SubscriptionArn
print(resp_email['SubscriptionArn'])

sns.list_subscriptions_by_topic(
  TopicArn = "arn:aws:sns:us-east-1:142389298461:city_alerts:93faf141-9b9e-42a1-a2a0-b85f7d78fbd1"
)

sns.list_subscriptions()['Subscriptions']

response = sns.list_subscriptions_by_topic(
  TopicArn='arn:aws:sns:us-east-1:142389298461:city_alerts')
  
subs = response['Subscriptions']
pprint(subs)



### 23
# # For each email in contacts, create subscription to street_critical
# for email in contacts['Email']:
#   sns.subscribe(TopicArn = str_critical_arn,
#                 # Set channel and recipient
#                 Protocol = 'email',
#                 Endpoint = email)

# # List subscriptions for streets_critical topic, convert to DataFrame
# response = sns.list_subscriptions_by_topic(
#   TopicArn = str_critical_arn)
# subs = pd.DataFrame(response['Subscriptions'])

# # Preview the DataFrame
# subs.head()



### 24
# # List subscriptions for streets_critical topic.
# response = sns.list_subscriptions_by_topic(
#   TopicArn = str_critical_arn)

# # For each subscription, if the protocol is SMS, unsubscribe
# for sub in response['Subscriptions']:
#   if sub['Protocol'] == 'sms':
# 	  sns.unsubscribe(SubscriptionArn=sub['SubscriptionArn'])

# # List subscriptions for streets_critical topic in one line
# subs = sns.list_subscriptions_by_topic(
#   TopicArn=str_critical_arn)['Subscriptions']

# # Print the subscriptions
# print(subs)



### 25
# # If there are over 100 potholes, create a message
# if streets_v_count > 100:
#   # The message should contain the number of potholes.
#   message = "There are {} potholes!".format(streets_v_count)
#   # The email subject should also contain number of potholes
#   subject = "Latest pothole count is {}".format(streets_v_count)

#   # Publish the email to the streets_critical topic
#   sns.publish(
#     TopicArn = str_critical_arn,
#     # Set subject and message
#     Message = message,
#     Subject = subject
#   )



### 26
# # Loop through every row in contacts
# for idx, row in contacts.iterrows():
    
#     # Publish an ad-hoc sms to the user's phone number
#     response = sns.publish(
#         # Set the phone number
#         PhoneNumber = str(row['Phone']),
#         # The message should include the user's name
#         Message = 'Hello {}'.format(row['Name'])
#     )
   
#     print(response)



### 27
# dept_arns = {} 

# for dept in departments:
#   # For each deparment, create a critical topic
#   critical = sns.create_topic(Name="{}_critical".format(dept))
#   # For each department, create an extreme topic
#   extreme = sns.create_topic(Name="{}_extreme".format(dept))
#   # Place the created TopicARNs into a dictionary 
#   dept_arns['{}_critical'.format(dept)] = critical['TopicArn']
#   dept_arns['{}_extreme'.format(dept)] = extreme['TopicArn']

# # Print the filled dictionary.
# print(dept_arns)