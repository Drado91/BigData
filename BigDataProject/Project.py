import ibm_boto3
from ibm_botocore.client import Config, ClientError
import sqlite3

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
        file_list=[]
        for file in files:
            file_name="Item: {0} ({1} bytes).".format(file.key, file.size)
            print(file_name)
            file_list.append(file_name)
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))
    return file_list

#mysal functions:
def mysql_create_db():
    con = sqlite3.connect('mydb_project.db')
    cur = con.cursor()
    return cur,con
def mysql_create_table(cur,table,col1,col2):
    sql_create_table = """ CREATE TABLE IF NOT EXISTS {} (
                                        {} text NOT NULL,
                                        {} text NOT NULL
                                    ); """.format(table,col1,col2)
    cur.execute(sql_create_table)
    print('Table {} has been created'.format(table))
def mysql_show_tables(mysql):
    mysql.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables=mysql.fetchall()
    tables = [item for t in tables for item in t]
    print(tables)
    return tables
def mysql_insert(con,cur,table,origin_name, name):
    if not(table in mysql_show_tables(cur)):
        print('table {} doesnt exist'.format(table))
    if mysql_get_name(cur,table,origin_name):
        print('ambigity {} exist on table {}'.format(origin_name,table))
        return
    sqlite_insert_query = "INSERT INTO {} VALUES ('{}','{}')".format(table,origin_name,name)
    count = cur.execute(sqlite_insert_query)
    con.commit()
    print("Record inserted successfully into SqliteDb_developers table ", cur.rowcount)
def mysql_print_rows(con,cur,table):
    for row in cur.execute('SELECT * FROM {} ORDER BY origin_name'.format(table)):
        print(row)
def mysql_drop_table(cursor,table):
    cursor.execute("DROP TABLE {}".format(table))
    print("DROP TABLE {}".format(table))
def mysql_get_name(cur,table,origin_name):
    item = cur.execute("SELECT name FROM {} WHERE origin_name = '{}'".format(table,origin_name)).fetchall()
    if len(item) > 1:
        print('ambiguity, please verify origin_name')
        print(item)
        return item
    elif len(item) == 1:
        print(item[0][0])
        return item[0][0]
    else:
        print('origin_name doesnt exist')
        return None
def mysql_get_origin_name(cur,table,name):
    item = cur.execute("SELECT origin_name FROM {} WHERE name = '{}'".format(table,name)).fetchall()
    if len(item) > 1:
        print('ambiguity, please verify name')
        print(item)
        return item
    elif len(item) == 1:
        print(item[0][0])
        return item[0][0]
    else:
        print('name doesnt exist')
        return None

#Required Functions:
def create_object(object_to_create,file_path,bucket_name="bucket-dudi-slava",):
    response=cos.meta.client.upload_file(object_to_create, bucket_name, file_path)
    #TODO:
    # 1. Origin file verfication
    #  size-optional, type-optional, existence-mandatory
    # 2. Destination file verfication
    #   existence,
    # 3. Bucket
    #   existence, list of buckets for user to choose.
    #response=bucket.put_object(Body=object_to_create)
    print(response)
def delete_object(object_to_delete):
    response=bucket.delete_objects(Delete={'Objects': [{'Key': object_to_delete}]})
    print(response)
def get_object(object_to_get, bucket_name = 'bucket-dudi-slava', ):
    #Code for download:
    """
    obj = bucket.Object(object_to_get)
    with open('filename', 'wb') as data:
        obj.download_fileobj(data)
    """
    #Code for get:
    return(cos.Object(bucket_name, object_to_get).get())
def create_directory(directory_name, path=""):
    response = bucket.put_object(Key=directory_name)

#create_bucket('bucket-dudi-slava')
bucket_name="bucket-dudi-slava"
bucket=cos.Bucket(bucket_name)
get_bucket_contents(bucket_name)
create_object('guru99.txt','guru123.txt')
f=get_object('guru123.txt')

mysql_cur,mysql_con=mysql_create_db()
table='names'
col1='origin_name'
col2='name'
mysql_create_table(mysql_cur,table,col1,col2)
mysql_show_tables(mysql_cur)
mysql_insert(mysql_con,mysql_cur,table,'origin_name11111222223333','name11111122222333333')
mysql_print_rows(mysql_con,mysql_cur,table)
name_to_get='origin_name11111222223333'
origin_name_to_get='name11111122222333333'
name=mysql_get_name(mysql_cur,table,name_to_get)
origian_name=mysql_get_origin_name(mysql_cur,table,origin_name_to_get)
#object = bucket.Object(bucket_name,'dudi)

print('Almost-Finish-Breakpoint')
"""
Write Python code with the prototype named “ExtendedObjectStorage” 
that will internally use MySQL database to support an atomic rename operation against object storage. 
ExtendedObjectStoage will expose an interface with “create_object, get_object, delete_object, create_directory, delete_directory, list_directory, rename_directory, rename_object”.

"""
