import csv
import json
import os

from flask import Flask, render_template, request

from Logging import Log
from db_cassandra import Cassandra
from db_mongo import Mongo
from db_mysql import Database

# Intializing the app name
app = Flask(__name__)

# Intializing the log
log = Log().create_log()

# Set the secret key to some random bytes. Keep this really secret!
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

# Global variable for connection object
connection_mongo, connection_sql, connection_cassandra = None, None, None


# For initalizing the connection
def initialize_mongo(request):
    try:
        global connection_mongo
        client_url = request.form['client_url']
        db_name = request.form['database_name']
        table_name = request.form['table_name']

        # Creating database object
        db = Mongo()

        # Creating a connection
        connection_mongo = db.create_connection(client_url)

        # Create a databse
        db_global = connection_mongo[db_name]

        # Create a table
        table_global = db_global[table_name]

        return connection_mongo, db_global, table_global
    except Exception as e:
        log.exception(e)
        render_template('error.html', e=e)


def initialize_sql(request):
    try:
        global connection_sql
        hostname = request.form['hostname']
        username = request.form['username']
        password = request.form['password']
        db_name = request.form['database_name']
        table_name = request.form['table_name']

        # Create a database object
        db = Database()

        # Create connection
        connection_sql = db.create_connection(hostname, username, password)

        # Create Cursor
        curr = db.create_cursor(connection_sql)

        return db, connection_sql, curr, db_name, table_name
    except Exception as e:
        log.exception(e)
        render_template('error.html', e=e)


def initialize_casssandra(request):
    try:
        global connection_cassandra
        username = request.form['username']
        password = request.form['password']
        cloud_config_path = request.form['secure_path']
        keyspace_name = request.form['keyspace_name']
        table_name = request.form['table_name']

        # Creating a database objecy
        db = Cassandra()

        # Creating a connection
        connection_cassandra = db.create_connection(username, password, cloud_config_path)

        # Create a database and use it
        connection_cassandra.execute('USE {}'.format(keyspace_name))

        return db, connection_cassandra, keyspace_name, table_name

    except Exception as e:
        log.exception(e)
        render_template('error.html', e=e)


# Home Route
@app.route('/', methods=['GET', 'POST'])
def homepage():
    return render_template('index.html')


# Rendering the HTML pages
@app.route('/<action_name>/render', methods=['GET', 'POST'])
def render_html(action_name):
    return render_template(action_name + '.html')


# Creating the table
@app.route('/<db_name>/create_table', methods=['POST'])
def create_table(db_name):
    # Check if it is a post request
    try:
        if request.method == 'POST':
            if (db_name == 'mongo'):

                connection_mongo, db_global, table_global = initialize_mongo(request)
                return render_template('success.html', success='Your operation is succesfully completed')

            elif (db_name == 'mysql'):

                db, connection_sql, curr, db_name, table_name = initialize_sql(request)
                col_names = request.form['column_names']
                col_dtypes = request.form['column_dtypes']

                #  Executing the query
                db.execute_query(curr, 'CREATE DATABASE IF NOT EXISTS {}'.format(db_name))
                db.execute_query(curr, 'USE {}'.format(db_name))

                col_list = col_names.split(",")
                col_dtypes = col_dtypes.split(',')
                if len(col_list) != len(col_dtypes):
                    return render_template('error.html',
                                           e='The length of column names and thier data type is not matching')
                query = 'CREATE TABLE {}('.format(table_name)

                # Query for defining the schema for the table
                for i in range(len(col_list)):
                    final_col_name = col_list[i] + ' ' + col_dtypes[i]
                    query += final_col_name
                    query += ','
                query = query[:-1] + ')'
                result = db.execute_query(curr, query)

                if not (len(result) == 2):
                    return render_template('success.html', success='Your operation is sucessfully completed')
                else:
                    log.exception(result[0])
                    return render_template('error.html', e=result[0])

            else:

                db, connection_cassandra, keyspace_name, table_name = initialize_casssandra(request)
                col_names = request.form['column_names']
                col_dtypes = request.form['column_dtypes']

                # Create a table
                col_names = col_names.split(',')
                col_dtypes = col_dtypes.split(',')
                if len(col_dtypes) != len(col_names):
                    return render_template('error.html', e='Length of columns are not matching')
                query = "CREATE TABLE {}(".format(table_name)

                # Creating query for table schema
                for i in range(len(col_names)):
                    final_col_names = col_names[i] + ' ' + col_dtypes[i]
                    query += final_col_names
                    query += ','
                query = query[:-1]
                query += ');'

                connection_cassandra.execute(query)

                return render_template('success.html', success='Your operation is sucessfully completed')


        else:
            log.exception('Trying to access method without POST request')
            render_template('error.html', e='Method is not allowed')
    except Exception as e:
        log.exception(e)
        return render_template('error.html', e=e)


# Inserting data into the table
@app.route('/<db_name>/insert_into_table', methods=['POST'])
def insert_table(db_name):
    try:
        if request.method == 'POST':
            if db_name == 'mongo':

                connection_mongo, db_global, table_global = initialize_mongo(request)
                record = request.form['record']
                record_json = json.loads(record)
                table_global.insert_one(record_json)

                return render_template('success.html', success='Your operation is sucessfully completed')


            elif db_name == 'mysql':

                db, connection_sql, curr, db_name, table_name = initialize_sql(request)
                row_value = request.form['row_value']
                row_dtype = request.form['row_dtype']

                # Query for inserting
                row_value = row_value.split(',')
                row_dtype = row_dtype.split(',')

                if (len(row_value) != len(row_dtype)):
                    render_template('error.html', e='Length is not matching')

                query = "INSERT INTO {}.{} VALUES(".format(db_name, table_name)

                for i in range(len(row_value)):
                    query = query + '\'' + row_value[i] + '\','
                query = query[:-1]
                query = query + ')'

                result = db.execute_query(curr, query)

                connection_sql.commit()
                if not (len(result) == 2):
                    return render_template('success.html', success='Your operation is sucessfully completed')
                else:
                    log.exception(result[0])
                    return render_template('error.html', e=result[0])

            else:

                db, connection_cassandra, keyspace_name, table_name = initialize_casssandra(request)
                column_names = request.form['column_names']
                row_values = request.form['row_values']

                connection_cassandra.execute('USE {}'.format(keyspace_name))

                # Forming the query
                query = 'INSERT INTO {}.{} ('.format(keyspace_name, table_name)
                row_value = row_values.split(',')
                column_names = column_names.split(',')

                if len(row_value) != len(column_names):
                    return render_template('error.html', e='Length is not matching')

                for i in range(len(column_names)):
                    query = query + column_names[i] + ','
                query = query[:-1] + ') values ('

                for i in range(len(row_value)):
                    query = query + '\'' + row_value[i] + '\'' + ','
                query = query[:-1] + ');'

                connection_cassandra.execute(query)

                return render_template('success.html', success='Your operation is sucessfully completed')

        else:
            log.exception('Method is not allowed')
            return render_template('error.html', e='Method is not allowed')
    except Exception as e:
        log.exception(e)
        return render_template('error.html', e=e)


# Update data into the table
@app.route('/<db_name>/update_into_table', methods=['POST'])
def update_table(db_name):
    try:
        if request.method == 'POST':

            if db_name == 'mongo':
                connection_mongo, db_global, table_global = initialize_mongo(request)
                filter_value = request.form['filter_value']
                set_value = request.form['set_value']
                filter_json = json.loads(filter_value)
                set_json = json.loads(set_value)
                table_global.update_one(filter_json, set_json)

                return render_template('success.html', success='Your operation is sucessfully completed')


            elif db_name == 'mysql':
                db, connection_sql, curr, db_name, table_name = initialize_sql(request)
                set_value = request.form['set_value']
                where_value = request.form['where_value']

                # Query for inserting
                set_value = set_value.split(',')
                where_value = where_value.split(',')

                if len(set_value) == 0:
                    return render_template('error.html', e='There is no value to set')

                query = "UPDATE {}.{} SET ".format(db_name, table_name)

                # Set value
                for i in range(len(set_value)):
                    query = query + set_value[i] + ','
                query = query[:-1]

                # Where value
                if len(where_value):
                    query = query + ' WHERE '
                for i in range(len(where_value)):
                    query = query + where_value[i]

                result = db.execute_query(curr, query)
                connection_sql.commit()
                if not (len(result) == 2):
                    return render_template('success.html', success='Your operation is sucessfully completed')
                else:
                    log.exception(result[0])
                    return render_template('error.html', e=result[0])
            else:

                db, connection_cassandra, keyspace_name, table_name = initialize_casssandra(request)
                set_value = request.form['set_value']
                where_value = request.form['where_value']

                # Forming the query
                query = 'UPDATE {}.{} ('.format(keyspace_name, table_name)
                set_value = set_value.split(',')
                where_value = where_value.split(',')

                if len(set_value) == 0:
                    return render_template('error.html', e='There is no value to set')

                query = "UPDATE {}.{} SET ".format(keyspace_name, table_name)

                # Set value
                for i in range(len(set_value)):
                    query = query + set_value[i] + ','
                query = query[:-1]

                # Where value
                if len(where_value):
                    query = query + ' WHERE '
                for i in range(len(where_value)):
                    query = query + where_value[i]

            connection_cassandra.execute(query)

            return render_template('success.html', success='Your operation is sucessfully completed')


        else:
            return render_template('error.html', e='Method is not allowed')
    except Exception as e:
        log.exception(e)
        return render_template('error.html', e=e)


# Bulk inserting into the table
@app.route('/<db_name>/bulk_insert', methods=['POST'])
def bulk_insert(db_name):
    try:
        if request.method == 'POST':
            if db_name == 'mongo':
                connection_mongo, db_global, table_global = initialize_mongo(request)
                if request.files:
                    file_to_upload = request.files['file_to_upload']

                else:
                    return render_template('error.html', e='File not uploaded')

                file_to_upload.save('bulk_mongo.json')

                with open('bulk_mongo.json', 'r') as f:
                    file_data = json.load(f)

                # Inserting the data into the collection
                if isinstance(file_data, list):
                    result = table_global.insert_many(file_data)
                else:
                    table_global.insert_one(file_data)

                return render_template('success.html', success='Your operation is sucessfully completed')


            elif db_name == 'mysql':
                db, connection_sql, curr, db_name, table_name = initialize_sql(request)
                if request.files:
                    file_to_upload = request.files['file_to_upload']
                else:
                    return render_template('error.html', e='File is not selected')

                # Saving file into the local
                file_path = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/bulk_sql.csv'
                file_to_upload.save(file_path)

                query = "LOAD DATA INFILE '{}' " \
                        "INTO TABLE {}.{} " \
                        "FIELDS TERMINATED BY ',' " \
                        "LINES TERMINATED BY '\n' " \
                        "IGNORE 1 ROWS;".format(file_path, db_name, table_name)

                result = db.execute_query(curr, query)
                connection_sql.commit()

                if len(result) != 2:
                    return render_template('success.html', success='Your operation is sucessfully completed')
                else:
                    return render_template('error.html', e=result[0])
            else:
                db, connection_cassandra, keyspace_name, table_name = initialize_casssandra(request)
                column_names = request.form['column_names']

                if request.files:
                    file_to_upload = request.files['file_to_upload']
                else:
                    return render_template('error.html', e='File is not selected')

                connection_cassandra.execute('USE {}'.format(keyspace_name))

                # Saving the file to local
                file_to_upload.save('bulk_cassandra.csv')
                column_names = column_names.split(',')
                col_names = '('

                for i in column_names:
                    col_names = col_names + i + ','
                col_names = col_names[:-1] + ') '

                # Forming the query
                with open('bulk_cassandra.csv', 'r') as data:
                    next(data)
                    data_csv = csv.reader(data, delimiter=',')
                    print(data_csv)

                    for i in data_csv:
                        counter = -1
                        query = "INSERT INTO {}.{} {} VALUES (".format(keyspace_name, table_name, col_names)
                        for entry in i:
                            query = query + '\'' + i[counter] + '\'' + ','
                            counter += 1
                        query = query[:-1] + ')'

                        connection_cassandra.execute(query)
                return render_template('success.html', success='Your operation is sucessfully completed')

        else:
            log.exception('Method is not allowed')
            return render_template('error.html', e='Method is not allowed')
    except Exception as e:
        log.exception(e)
        return render_template('error.html', e=e)


# Delete data from the table
@app.route('/<db_name>/delete_from_table', methods=['POST'])
def delete_table(db_name):
    try:
        if request.method == 'POST':

            if db_name == 'mongo':
                connection_mongo, db_global, table_global = initialize_mongo(request)
                filter_value = request.form['filter_value']

                filter_json = json.loads(filter_value)

                table_global.delete_one(filter_json)

                return render_template('success.html', success='Your operation is succesfully completed')

            elif db_name == 'mysql':
                db, connection_sql, curr, db_name, table_name = initialize_sql(request)
                where_value = request.form['where_value']

                # Query for deleting
                where_value = where_value.split(',')

                query = "DELETE FROM {}.{} ".format(db_name, table_name)

                # Where value
                if len(where_value):
                    query = query + ' WHERE '
                for i in range(len(where_value)):
                    query = query + where_value[i]
                print(query)
                result = db.execute_query(curr, query)
                connection_sql.commit()

                if len(result) != 2:
                    return render_template('success.html', success='Your operation is sucessfully completed')
                else:
                    return render_template('error.html', e=result[0])
            else:
                db, connection_cassandra, keyspace_name, table_name = initialize_casssandra(request)
                where_value = request.form['where_value']

                connection_cassandra.execute('USE {}'.format(keyspace_name))

                # Forming the query
                query = 'DELETE FROM {}.{} '.format(keyspace_name, table_name)

                where_value = where_value.split(',')

                # Where value
                if len(where_value):
                    query = query + ' WHERE '
                for i in range(len(where_value)):
                    query = query + where_value[i]
                query += ';'

                connection_cassandra.execute(query)

                return render_template('success.html', success='Your operation is succesfully completed')
        else:
            return render_template('error.html', e='Method is not allowed')
    except Exception as e:
        return render_template('error.html', e=e)


# Downloading the data from table
@app.route('/<db_name>/download', methods=['POST'])
def download(db_name):
    try:
        if request.method == 'POST':

            if db_name == 'mongo':
                connection_mongo, db_global, table_global = initialize_mongo(request)
                filter_value = request.form['filter_value']

                if filter_value:
                    filter_json = json.loads(filter_value)
                else:
                    filter_json = {}

                result = table_global.find(filter_json)

                with open('download_mongo.txt', 'w') as file:
                    for i in result:
                        i.pop('_id')
                        json.dump(i, file)

                return render_template('success.html',
                                       success='Downloading is completed. You can find that in {}location'.format(
                                           os.getcwd()))

            elif db_name == 'mysql':
                db, connection_sql, curr, db_name, table_name = initialize_sql(request)
                where_value = request.form['where_value']

                # Query for deleting
                if len(where_value):
                    where_value = where_value.split(',')

                query = "SELECT * FROM {}.{} ".format(db_name, table_name)

                # Where value
                if len(where_value):
                    query = query + ' WHERE '
                for i in range(len(where_value)):
                    query = query + where_value[i]

                curr.execute(query)

                with open('download_sql.csv', 'w') as file:
                    writer = csv.writer(file)
                    for i in curr.fetchall():
                        writer.writerow(list(i))

                return render_template('success.html',
                                       success='Downloading is completed. You can find that in {}location'.format(
                                           os.getcwd()))
            else:
                db, connection_cassandra, keyspace_name, table_name = initialize_casssandra(request)
                where_value = request.form['where_value']

                connection_cassandra.execute('USE {}'.format(keyspace_name))

                if len(where_value):
                    where_value = where_value.split(',')

                # Forming the select query
                query = 'SELECT * FROM {}.{}'.format(keyspace_name, table_name)

                # Where value
                if len(where_value):
                    query = query + ' WHERE '
                for i in range(len(where_value)):
                    query = query + where_value[i]
                query += ';'
                print(query)

                result = connection_cassandra.execute(query)

                # Writing into the file
                with open('download_cassandra.csv', 'w') as file:
                    writer = csv.writer(file)
                    for i in result:
                        writer.writerow(i)

                return render_template('success.html',
                                       success='Downloading is completed. You can find that in {}location'.format(
                                           os.getcwd()))
        else:
            return render_template('error.html', e='Method is not allowed')
    except Exception as e:
        return render_template('error.html', e=e)


if __name__ == '__main__':
    app.run(debug=True)
