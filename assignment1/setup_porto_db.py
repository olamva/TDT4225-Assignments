from utils.DbConnector import DbConnector

db = DbConnector(HOST="localhost",
                 DATABASE="porto",
                 USER="root",
                 PASSWORD="secret")

db.cursor.execute("SHOW TABLES;")
print(db.cursor.fetchall())

db.close_connection()
