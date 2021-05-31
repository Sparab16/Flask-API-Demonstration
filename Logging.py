import logging as log
log.basicConfig(filename='flask.log', level = log.DEBUG )

class Log:
    def create_log(self):
        return log