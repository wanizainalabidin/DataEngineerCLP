import csv
import psycopg2
from tabulate import tabulate
from flask import Flask, render_template, request, redirect, url_for
import os
from flask import Flask, request, render_template, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
from datetime import datetime



#Environment variable to connect to PSQL
USER = "postgres"
PASSWORD = "postgres"
PORT = 5432

# build the flask app by defining database connection to docker
app = Flask(__name__, template_folder="template")



# Create index route where end users can go to; a welcome page
@app.route('/')
def index():
    return "Welcome to Database Sensor. On the server, type /get to get current data from Postgres! Or /Upload to upload a file. To see the updated database after uploading the file, use /update"


#This request is for users to understand current, existing data that is already available in Postgres.
@app.route('/get', methods=["GET"])
def get_database():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM stream;')
    stream1 = cur.fetchall()
    stream2 = tabulate(stream1, tablefmt='html')
    cur.close()
    conn.close()
    return stream2


# A HTML file to allow users to upload a file. In this case, only CSV file is accepted. The uploaded file will be automatically downloaded and stored in current directory
ALLOWED_EXTENSIONS = ["csv"]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/upload', methods = ["GET","POST"])
def upload():
    if request.method =="POST":
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            dir=os.getcwd()   
            save_location = os.path.join(dir, filename) 
            file.save(save_location)
            return "Thank you for uploading"
    return render_template("index.html")


def get_db_connection():
    conn = psycopg2.connect(host='localhost',
                            database='clp',
                            user=USER,
                            password=PASSWORD, port =PORT)
    return conn


#This HTTP request helps users understand the updated database after user has uploaded the file. 
#Currently, the file is read manually but, moving forward, it will be good to read files dynamically once user has uploaded the file
@app.route('/update', methods=["PUT"])
def create_database():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        with open('Input/clp dummy-2.csv', 'r') as f:
            dummy = csv.reader(f)
            next(dummy)
            cur.copy_from(f, "stream", sep=",")
            cur.execute('SELECT * FROM stream;')
            stream = cur.fetchall()
            stream1 = tabulate(stream, tablefmt='html')
            cur.close()
            conn.close()
            return stream1 #when users type /get in the local host port, this will display the database in tabular format
#                        #since the question has asked for passing a CSV file that can populate to a database, this will work.
#                       #In the future, the code can be further extended to include other CRUD commands together with POST, PUT and delete
#
    except Exception:
        return "ERROR: Please Check if Sensor_ID is unique and data types are matching with database"
