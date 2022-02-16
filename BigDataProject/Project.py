import ibm_boto3
from ibm_botocore.client import Config, ClientError
import sqlite3
import time

# Constants for IBM COS values
COS_ENDPOINT = "https://s3.eu.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
COS_API_KEY_ID = "sZnC-SV3PS4OFS0ns5N2uUoVHU-m9N4hUl4Y6P_I_nAM" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/0767c658d1ba4bd69ba5626c95617d42:b72f9df1-e0fc-497e-aa5c-5de298e1bdee::" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"
COS_BUCKET_LOCATION = "eu-geo"


# A simple database wrapper class based on a local sqlite file. The database schema is <key, value> where key is the
# most up to date object name and the value is the initial object name and its actual name on the object storage.
class Database:
    def __init__(self):
        self.conn = sqlite3.connect('mydb_project.db')
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS renames_table;")
        cur.execute("CREATE TABLE renames_table (key text NOT NULL, value text NOT NULL)")
        print('Database is set')

    def insert_key_value(self, key, value):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO renames_table VALUES ('{}','{}')".format(key, value))
        self.conn.commit()
        print(f'Inserted {key}:{value}')

    def delete_key(self, key):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM renames_table WHERE key = '{}'".format(key))
        self.conn.commit()
        print(f'Deleted {key}')

    def update_key(self, old_key, new_key):
        cur = self.conn.cursor()
        cur.execute("UPDATE renames_table SET key = '{}' WHERE key = '{}'".format(new_key, old_key))
        self.conn.commit()
        print(f'Updated {old_key}:{new_key}')

    def get_value(self, key):
        cur = self.conn.cursor()
        item = cur.execute("SELECT value FROM renames_table WHERE key = '{}'".format(key)).fetchall()
        if item and item[0]:
            return item[0][0]
        return None

    def get_keys_with_prefix(self, prefix):
        cur = self.conn.cursor()
        items = cur.execute("SELECT key FROM renames_table WHERE key LIKE '{}%'".format(prefix)).fetchall()
        if items:
            items_list = []
            for item in items:
                items_list.append(item[0])
            return items_list
        return None

    def print_db(self):
        cur = self.conn.cursor()
        for row in cur.execute('SELECT * FROM renames_table ORDER BY key'):
            print(row)


# An object storage interface that supports an atomic rename operation by using an additional database. Directory
# operations are supported by treating directories as object prefixes.
class ExtendedObjectStorage:
    def __init__(self):
        self.cos = ibm_boto3.resource("s3",
                                      ibm_api_key_id=COS_API_KEY_ID,
                                      ibm_service_instance_id=COS_INSTANCE_CRN,
                                      config=Config(signature_version="oauth"),
                                      endpoint_url=COS_ENDPOINT)
        ts = time.time()
        self.bucket_name = f'bucket-{ts}'
        print("Creating new bucket: {0}".format(self.bucket_name))
        self.cos.Bucket(self.bucket_name).create(
            CreateBucketConfiguration={
                "LocationConstraint": COS_BUCKET_LOCATION
            }
        )
        print("Bucket: {0} created!".format(self.bucket_name))
        self.db = Database()
        print("ExtendedObjectStorage: all set")

    # Create a new object on the object storage and a new record in the renames table with <object name, object name>.
    def create_object(self, object_name, object_content):
        print("Creating new object: {0}".format(object_name))
        self.cos.Object(self.bucket_name, object_name).put(
            Body=object_content
        )
        # We don't actually need to store the object in the database when it's created but only when it's renamed. We
        # still do that to simplify the prototype. If we didn't insert all the objects we would have needed to add
        # existence checks and error handling in the other methods like delete_object and this would complicate the code
        # unnecessarily.
        self.db.insert_key_value(object_name, object_name)
        print("Object: {0} created!".format(object_name))

    # Fetches the most up to date object name from the database and then fetches the object.
    def get_object(self, object_name):
        print("Retrieving object from bucket: {0}, key: {1}".format(self.bucket_name, object_name))
        object_name_in_bucket = self.db.get_value(object_name)
        if object_name_in_bucket is None:
            print("No such object")
        else:
            obj = self.cos.Object(self.bucket_name, object_name_in_bucket).get()
            print("Object contents: {0}".format(obj["Body"].read()))

    # Fetches the most up to date object name from the database and then deletes the object.
    def delete_object(self, object_name):
        self.cos.Object(self.bucket_name, object_name).delete()
        self.db.delete_key(object_name)
        print("Object: {0} deleted!\n".format(object_name))

    # Updates the most up to date name of the object to a new name in the database. Doesn't change the actual object.
    def rename_object(self, old_object_name, new_object_name):
        self.db.update_key(old_object_name, new_object_name)

    # Directories do not exist in object storage. Therefore, creating a directory is meaningless and this is a noop
    # operation (we write some dummy file to the object storage).
    def create_directory(self, directory_name):
        print("Creating new directory: {0}".format(directory_name))
        self.cos.Object(self.bucket_name, directory_name).put(
            Body=""
        )
        print("Directory: {0} created!".format(directory_name))

    # Fetches the most up to date object names that have a similar prefix to the directory name from the database and
    # returns them.
    def list_directory(self, directory_name):
        print("Listing directory: {0}".format(directory_name))
        objs = self.db.get_keys_with_prefix(directory_name)
        print(objs)
        return objs

    # Fetches the most up to date object names that have a similar prefix to the directory name from the database and
    # deletes them.
    def delete_directory(self, directory_name):
        obj_list = self.list_directory(directory_name)
        # This is for simplicity, we can use a single delete many operation here.
        for obj in obj_list:
            self.delete_object(obj)

    # Fetches the most up to date object names that have a similar prefix to the directory name from the database and
    # renames them.
    def rename_directory(self, old_directory_name, new_directory_name):
        obj_list = self.list_directory(old_directory_name)
        for obj in obj_list:
            self.rename_object(obj, obj.replace(old_directory_name, new_directory_name))


dir1_name = "/dir1"
dir2_name = "/dir1/dir2"
object1_name = "/dir1/dir2/object1.txt"
object2_name = "/dir1/dir2/object2.txt"

# Database operations test suite
db = Database()
db.insert_key_value(object1_name, object1_name)
db.update_key(object1_name, object2_name)
print(db.get_value(object1_name))
print(db.get_value(object2_name))
print(db.get_keys_with_prefix(dir1_name))
print(db.get_keys_with_prefix(dir2_name))
print(db.get_keys_with_prefix("invalid"))
db.delete_key(object2_name)
db.print_db()

# Objects operations test suite
eos = ExtendedObjectStorage()
eos.create_object(object1_name, "I'm a nice object")
eos.get_object(object1_name)
eos.delete_object(object1_name)
eos.get_object(object1_name)
eos.create_object(object1_name, "I'm a nice object")
eos.get_object(object1_name)
eos.rename_object(object1_name, object2_name)
eos.get_object(object1_name)
eos.get_object(object2_name)

# Directories operations test suite
eos = ExtendedObjectStorage()
eos.list_directory(dir2_name)
eos.create_object(object1_name, "I'm a nice object")
eos.list_directory(dir2_name)
eos.create_object(object2_name, "I'm a nice object")
eos.list_directory(dir2_name)
eos.rename_directory(dir2_name, dir1_name)
eos.list_directory(dir2_name)
eos.list_directory(dir1_name)
eos.delete_directory(dir1_name)
eos.list_directory(dir1_name)

print('Done')
