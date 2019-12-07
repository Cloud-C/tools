import os


class Config:
    SECRET_KEY = os.environ['SECRET_KEY']
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    SQLALCHEMY_BINDS = {
        'mysql': 'mysql+pymysql://{usr}:{pwd}@{host}:{port}/{db}'.format(
            usr=os.environ['MYSQL_USER'],
            pwd=os.environ['MYSQL_PASSWORD'],
            host=os.environ['MYSQL_HOST'],
            port=os.environ['MYSQL_PORT'],
            db=os.environ['DB_NAME']
        ),
    }
    REDIS_HOST = os.environ.get('REDIS_HOST', None)
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
    REDIS_PORT = os.environ['REDIS_PORT']
    SYSTEM_NAME = 'system_name'
