from pymongo import MongoClient


class DbConnector:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "localhost" // Your local MongoDB Docker container
    USER = "TEST_USER" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """
    is_sepanta = False

    def __init__(self,
                 DATABASE='DATABASE_NAME',
                 HOST="127.0.0.1",
                 USER="TEST_USER",
                 PASSWORD="test123"):
        if self.is_sepanta:
            # Windows-compatible: No authentication for local development
            uri = "mongodb://%s:27017/" % HOST
        else:
            # Mac/Linux: With authentication
            uri = "mongodb://%s:%s@%s/?authSource=admin" % (USER, PASSWORD, HOST)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
