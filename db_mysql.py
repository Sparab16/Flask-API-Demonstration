# Importing the libraries
import mysql.connector as connection
from flask import render_template
from Logging import Log


# Initialize the log
log = Log().create_log()

class Database:

    def create_connection(self, host, user, passwd):
        try:
            database = connection.connect(host=host, user=user, passwd=passwd, use_pure=True, auth_plugin='mysql_native_password')
            return database
        except Exception as e:
            log.exception(e)
            render_template('error.html', e = e)
            return [e, False]

    def create_cursor(self, connection):
        try:
            return connection.cursor()
        except Exception as e:
            log.exception(e)
            render_template('error.html', e = e)
            return [e, False]

    def execute_query(self, curr, query):
        try:
            curr.execute(query)
            return [True]
        except Exception as e:
            log.exception(e)
            render_template('error.html', e = e)
            return [e, False]
