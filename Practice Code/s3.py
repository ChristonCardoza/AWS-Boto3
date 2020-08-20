import boto3
import random

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # Bucket Operation
    
    # bucket_name = "chris"+str(random.randint(0,100))
    # create_response = create_bucket(s3,bucket_name)
    # display_respnse = displaying_buckets(s3)
    # delete_response = delete_bucket(s3,random.choice(display_respnse))
    # display_response = displaying_buckets(s3)
    # upload_response = upload_file(s3,"/var/task/Picture.jpg",bucket_name,'Joker.jpg')
    bucket_name = 'chris63'
    destination_bucket_name = 'deadly3'
    
    # Bucket Object Operation
    
    bucket_object = 'new_img'+str(random.randint(0,100))+'.jpg'
    list_iems_response = list_items(s3,bucket_name)
    # get_item_response = get_item(s3,bucket_name,random.choice(list_iems_response))
    # put_item_response = put_item(s3,get_item_response,bucket_name,bucket_object)
    # delete_item_response = delete_item(s3,bucket_name,random.choice(list_iems_response))
    # copy_item_response = copy_item(s3,destination_bucket_name,{'Bucket': bucket_name, 'Key': random.choice(list_iems_response)})
    download_item_response = download_item(s3,bucket_name, random.choice(list_iems_response),'/tmp/'+'down-'+bucket_object)
    
def create_bucket(obj,name):
    print("*"*100)
    print("Creating bucket")
    delete = obj.create_bucket(Bucket=name)
    print("*"*100)
    return delete
    
def  delete_bucket(obj,name):
    print("*"*100)
    print("Deleting bucket")
    create = obj.delete_bucket(Bucket=name)
    print("*"*100)
    return create
    
def displaying_buckets(obj):
    print("*"*100)
    print("The Available buckets are :")
    buckets_names = []
    buckets_info = obj.list_buckets()
    for i in range(len(buckets_info['Buckets'])):
        buckets_names.append(buckets_info['Buckets'][i]['Name'])
    print(buckets_names)
    print("*"*100)
    return buckets_names
    
def upload_file(obj,filepath,bucket_name,filename):
    print("*"*100)
    print("Uploading file")
    upload = obj.upload_file(filepath,bucket_name,filename)
    print(upload)
    print("*"*100)
    return upload

def list_items(obj,bucket_name):
    print("*"*100)
    print("list outing file present in {} bucket".format(bucket_name))
    items_name = []
    items = obj.list_objects(Bucket=bucket_name)
    if items.get('Contents','Not Exists') != 'Not Exists':
        for i in range(len(items['Contents'])):
            items_name.append(items['Contents'][i]['Key'])
        print("There are total {} numbers of object which are :{} ".format(str(len(items['Contents'])),str(items_name)))
        return items_name
    else:
         items_name.append("Empty Bucket!!!")
         print("There are O objectin the buckets")
         return items_name
    
        
def get_item(obj,bucket_name,items_name):
    print("*"*100)
    print("Getting item {} from {} bucket".format(items_name,bucket_name))
    item = obj.get_object(Bucket=bucket_name,Key=items_name)
    item_data =item['Body'].read()
    print(item_data)
    print("*"*100)
    returnitem_data
    
def put_item(obj,item_body,bucket_name,items_name):
    print("*"*100)
    print("Putting item {} to {} bucket".format(items_name,bucket_name))
    item = obj.put_object(Body=item_body,Bucket=bucket_name,Key=items_name)
    print(item)
    print("*"*100)
    return  item
    
def delete_item(obj,bucket_name,items_name):
    print("*"*100)
    print("Deleting item {} to {} bucket".format(items_name,bucket_name))
    item = obj.delete_object(Bucket=bucket_name,Key=items_name)
    print(item)
    print("*"*100)
    return  item
    
def copy_item(obj,dest_bucket,src_bucket):
    print("*"*100)
    print("Copying item {} from {} bucket to {} bucket".format(src_bucket['Key'],src_bucket['Bucket'],dest_bucket))
    item = obj.copy_object(Bucket=dest_bucket,CopySource=src_bucket,Key=src_bucket['Key'])
    print(item)
    print("*"*100)
    return  item
    
def download_item(obj,bucket_name,items_name,download_path):
    print("*"*100)
    print("Downloading item {} from {} bucket".format(items_name,bucket_name))
    item = obj.download_file(Bucket=bucket_name, Key=items_name, Filename=download_path)
    print(item)
    print("*"*100)
    return  item    

   
