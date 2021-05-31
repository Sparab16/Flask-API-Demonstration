from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from flask import render_template

from Logging import Log

# Init the log
log = Log().create_log()


class Cassandra:

    def create_connection(self, username, password, secure_bundle_path):
        try:
            cloud_config = {
                'secure_connect_bundle': secure_bundle_path
            }
            auth_provider = PlainTextAuthProvider(username, password)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            return session

        except Exception as e:
            log.exception(e)
            render_template('error.html', e=e)
            return [e, False]
