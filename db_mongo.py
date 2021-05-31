from flask import render_template
import pymongo
from Logging import Log

# Init the log
log = Log().create_log()

class Mongo:

    def create_connection(self, client_url):
        try:
            client = pymongo.MongoClient(client_url)
            return client
        except Exception as e:
            log.exception(e)
            render_template('error.html', e = e)
            return [e, False]
