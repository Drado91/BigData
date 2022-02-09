import ibm_boto3
from ibm_botocore.client import Config, ClientError

# Constants for IBM COS values
COS_ENDPOINT = "https://s3.eu.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
COS_API_KEY_ID = "sZnC-SV3PS4OFS0ns5N2uUoVHU-m9N4hUl4Y6P_I_nAM" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/0767c658d1ba4bd69ba5626c95617d42:b72f9df1-e0fc-497e-aa5c-5de298e1bdee::" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"
COS_BUCKET_LOCATION = "eu-geo"
# Create resource
cos = ibm_boto3.resource("s3",
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT)

#Temporarly functions:
def create_bucket(bucket_name):
    print("Creating new bucket: {0}".format(bucket_name))
    try:
        cos.Bucket(bucket_name).create(
            CreateBucketConfiguration={
                "LocationConstraint":COS_BUCKET_LOCATION
            }
        )
        print("Bucket: {0} created!".format(bucket_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to create bucket: {0}".format(e))
def create_text_file(bucket_name, item_name, file_text):
    print("Creating new item: {0}".format(item_name))
    try:
        cos.Object(bucket_name, item_name).put(
            Body=file_text
        )
        print("Item: {0} created!".format(item_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to create text file: {0}".format(e))
def get_buckets():
    print("Retrieving list of buckets")
    try:
        buckets = cos.buckets.all()
        for bucket in buckets:
            print("Bucket Name: {0}".format(bucket.name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve list buckets: {0}".format(e))
def get_bucket_contents(bucket_name):
    print("Retrieving bucket contents from: {0}".format(bucket_name))
    try:
        files = cos.Bucket(bucket_name).objects.all()
        for file in files:
            print("Item: {0} ({1} bytes).".format(file.key, file.size))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))

#Required Functions:
def create_object(object_to_create):
    response=bucket.put_object(Key=object_to_create)
    print(response)

def delete_object(object_to_delete):
    response=bucket.delete_objects(Delete={'Objects': [{'Key': object_to_delete}]})
    print(response)

"""
f= open("guru999.txt","w+")
for i in range(10):
     f.write("This is line %d\r\n" % (i+1))
"""
#create_bucket('bucket-dudi-slava')
bucket_name="bucket-dudi-slava"
bucket=cos.Bucket(bucket_name)
get_bucket_contents(bucket_name)
#object = bucket.Object(bucket_name,'dudi)


"""
Write Python code with the prototype named “ExtendedObjectStorage” 
that will internally use MySQL database to support an atomic rename operation against object storage. 
ExtendedObjectStoage will expose an interface with “create_object, get_object, delete_object, create_directory, delete_directory, list_directory, rename_directory, rename_object”.

"""

print('hi')