import redis
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import Config


app = Flask(__name__)
config = Config()
app.config.from_object(config)

rs = redis.StrictRedis(
    host=config.REDIS_HOST, port=config.REDIS_PORT, password=config.REDIS_PASSWORD,
    charset='utf-8', decode_responses=True)

db = SQLAlchemy(app)


def create_app():
    return app
