import json
from datetime import datetime, date
from app import rs


class CacheModel:
    """
        cache 物件殼
    """
    def __init__(self, _class, **kwargs):
        self.__dict__ = kwargs
        self._class = _class

    def __repr__(self):
        return f'<{self.__class__.__module__}.{self.__class__.__name__} ({self._class}) object at {hex(id(self))}>'


class CacheTool:

    @staticmethod
    def convert_datetime_to_str(_datetime):
        return _datetime.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def convert_str_to_datetime(_str):
        return datetime.strptime(_str, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def convert_date_to_str(_datetime):
        return _datetime.strftime('%Y-%m-%d')

    @staticmethod
    def convert_str_to_date(_str):
        return datetime.strptime(_str, '%Y-%m-%d').date()

    @staticmethod
    def format_datetime_key_for_search(_datetime):
        """
            以日分割的cache key
        """
        return _datetime.strftime('%Y-%m-%d')

    @classmethod
    def model_to_dict(cls, model_obj, data_fields):
        temp_dict = dict()
        for column, data_type in data_fields.items():
            value = getattr(model_obj, column, None)
            if value:
                if data_type == datetime:
                    value = cls.convert_datetime_to_str(_datetime=value)
                elif data_type == date:
                    value = cls.convert_date_to_str(_datetime=value)
            temp_dict.update({
                column: value
            })
        return temp_dict

    @classmethod
    def dict_to_model(cls, data_dict, data_fields, class_name=None):
        temp_dict = dict()
        for column, data_type in data_fields.items():
            value = data_dict.get(column, None)
            if value:
                if data_type == datetime:
                    value = cls.convert_str_to_datetime(_str=value)
                elif data_type == date:
                    value = cls.convert_str_to_date(_str=value)
            temp_dict.update({
                column: value
            })
        cache_model = CacheModel(_class=class_name, **temp_dict)
        return cache_model

    @staticmethod
    def validate_key_set(key_set, data_fields):
        for key in key_set:
            if key not in data_fields:
                raise Exception(f'"{key}" not in fields: {set(data_fields)}')

    @staticmethod
    def get_data_fields(model):
        table_obj = model.__dict__
        data_fields = dict()
        for key, value in table_obj.items():
            if key.startswith('_'):
                continue
            value_dict = value.__dict__
            comparator = value_dict['comparator']
            comparator_dict = comparator.__dict__
            data_type = comparator_dict.get('type', None)
            python_data_type = getattr(data_type, 'python_type', None)
            if python_data_type:
                data_fields.update({
                    key: python_data_type
                })
        return data_fields


class CacheSetter:
    """
        製造cache

        指定cache不過期: expire_time設為None

        example: <model> = <SQLAlchemy_Model>

            cache_setter = CacheSetter(model=<model>, key_set={<col_name_1>, <col_name_2>, ...})
            cache_setter.filter().exec()
            cache_setter.filter(<model>.<col_name_1> == <something>, <model>.<col_name_2> == <something>).exec()
            cache_setter.filter(<model>.<col_name_1> == <something>).filter(<model>.<col_name_2> == <something>).exec()
            cache_setter.filter().order_by(<model>.<col_name_1>.desc()).limit(<limit_size>).exec()
    """
    def __init__(self, model, key_set=None, special_key=None, expire_time=60 * 60 * 24 * 30):
        self.model = model
        self.model_name = self.model.__tablename__
        self.key_set = key_set
        self.special_key = special_key
        self.redis_pipeline = self._get_pipeline()
        self.expire_time = expire_time
        self.data_fields = CacheTool.get_data_fields(model=self.model)
        self.obj_count = 0
        self.order_conditions = list()
        self.filter_conditions = list()
        self.limit_size = None
        self._validate()

    def _validate(self):
        if not (self.key_set or self.special_key):
            raise Exception('need attribute "key_set" or "special_key"')
        if self.special_key:
            self.special_key = str(self.special_key)
        if self.key_set:
            if type(self.key_set) is not set:
                raise Exception('attribute "key_set" type must be "set"')
            else:
                CacheTool.validate_key_set(key_set=self.key_set, data_fields=self.data_fields)

    def _get_redis_key(self, attr_key):
        return f'common_cache:{self.model_name}:{attr_key}'

    @staticmethod
    def _get_attr_key(key, value):
        return f'{key}:{value}'

    @staticmethod
    def _get_pipeline():
        return rs.pipeline()

    @staticmethod
    def _base_query_to_list(base_query_obj):
        """
            使用limit查詢時回傳的類別是BaseQuery類 非list
            故迭代此類丟進list裡傳回
        """
        return [obj for obj in base_query_obj]

    def _query(self):
        temp_query = self.model.query
        if self.filter_conditions:
            temp_query = temp_query.filter(*self.filter_conditions)
        if self.order_conditions:
            temp_query = temp_query.order_by(*self.order_conditions)
        if self.limit_size is None:
            return self._base_query_to_list(base_query_obj=temp_query.limit(self.limit_size))
        return temp_query.all()

    def _format_redis_key(self, key, model_obj, obj_dict):
        key_type = self.data_fields.get(key, None)
        if not key_type:
            return None
        if key_type == datetime:
            value = CacheTool.format_datetime_key_for_search(_datetime=getattr(model_obj, key))
        else:
            value = obj_dict.get(key, None)
        return f'{key}:{value}'

    def _set_to_redis_pipeline(self, key, value):
        self.redis_pipeline.set(name=key, value=value, ex=self.expire_time)

    def filter(self, *conditions):
        self.filter_conditions.extend(conditions)
        return self

    def order_by(self, *conditions):
        self.order_conditions.extend(conditions)
        return self

    def limit(self, size):
        self.limit_size = size
        return self

    @staticmethod
    def _update_dict(_dict, key, obj):
        if key in _dict:
            value_dict = _dict[key]
            value_dict.append(obj)
        else:
            _dict.update({
                key: [obj]
            })
        return _dict

    def _category_by_key_set(self, model_obj_list):
        temp_dict = dict()
        for model_obj in model_obj_list:
            obj_dict = CacheTool.model_to_dict(model_obj=model_obj, data_fields=self.data_fields)
            temp_dict = self._update_dict(_dict=temp_dict, key=self.special_key, obj=obj_dict)
            if not self.key_set:
                continue
            for key in self.key_set:
                attr_key = self._format_redis_key(key=key, model_obj=model_obj, obj_dict=obj_dict)
                if not attr_key:
                    continue
                temp_dict = self._update_dict(_dict=temp_dict, key=attr_key, obj=obj_dict)
        return temp_dict

    def exec(self):
        model_obj_list = self._query()
        if not model_obj_list:
            return
        self.obj_count = len(model_obj_list)
        category_dict = self._category_by_key_set(model_obj_list=model_obj_list)
        if not category_dict:
            return
        for attr_key, model_obj_list in category_dict.items():
            redis_key = self._get_redis_key(attr_key=attr_key)
            self._set_to_redis_pipeline(key=redis_key, value=json.dumps(model_obj_list))
        self.redis_pipeline.execute()

    def count(self):
        return self.obj_count


class CacheGetter:
    """
        取得cache

        example: <model> = <SQLAlchemy_Model>

            cache_getter = CacheGetter(model=<model>)
            result = cache_getter.get(key=<col_name_1>, attr=<attr_1>)
            result_list = cache_getter.get(key=<col_name_2>, attr=<attr_2>)
    """
    def __init__(self, model):
        self.model = model
        self.model_name = self.model.__tablename__
        self.data_fields = CacheTool.get_data_fields(model=self.model)

    def _get_redis_key(self, key, attr):
        return f'common_cache:{self.model_name}:{key}:{attr}'

    def _wrap_to_model(self, result_list):
        temp_list = list()
        for data_dict in result_list:
            model_obj = CacheTool.dict_to_model(
                data_dict=data_dict, data_fields=self.data_fields, class_name=self.model.__name__)
            temp_list.append(model_obj)
        return temp_list

    def _exec(self, key, attr, to_model=False):
        redis_key = self._get_redis_key(key=key, attr=attr)
        result_str = rs.get(redis_key)
        if not result_str:
            return None
        result_list = json.loads(result_str)
        if to_model:
            result_list = self._wrap_to_model(result_list=result_list)
        result_len = len(result_list)
        if result_len == 0:
            return None
        if result_len == 1:
            return result_list[0]
        return result_list

    def get_dict(self, key, attr):
        return self._exec(key=key, attr=attr)

    def get_obj(self, key, attr):
        return self._exec(key=key, attr=attr, to_model=True)
