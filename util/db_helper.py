from app import db
from util.debugtool import DebugTool


class DBHelper:
    
    @staticmethod
    def commit(msg=None):
        try:
            db.session.commit()
        except Exception as e:
            DebugTool.error(e, msg=msg)
            db.session.rollback()
            raise e

    @staticmethod
    def expunge_obj(obj):
        if obj:
            db.session.expunge(obj)
        return obj

    @staticmethod
    def expunge_list(obj_list):
        if obj_list:
            temp_list = list()
            for obj in obj_list:
                db.session.expunge(obj)
                temp_list.append(obj)
            obj_list = temp_list
        return obj_list

    @classmethod
    def trans_model_list(cls, model_list):
        temp_list = list()
        for model in model_list:
            trans_model = cls.trans_model(model)
            temp_list.append(trans_model)
        return temp_list

    @staticmethod
    def trans_model(model):
        temp_dict = dict()
        attr_dict = model.__dict__
        for key, value in attr_dict.items():
            if key[:1] != '_':
                temp_dict[key] = value
        return ProxyModel(**temp_dict)

    @staticmethod
    def bulk_update(model, data):
        try:
            db.session.bulk_update_mappings(model, data)
            db.session.commit()
        except Exception as e:
            DebugTool.error(e)
            db.session.rollback()
            raise e


# 代理ORM Model
class ProxyModel:
    def __init__(self, **kwargs):
        self.__dict__ = kwargs
