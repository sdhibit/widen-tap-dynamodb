import datetime
import simplejson as json
import re

from singer_sdk import typing as th


def merge_schemas(schema1: dict, schema2: dict) -> dict:
    """Merge two existing schemas such that nothing is lost between the two."""

    if schema1 is None:
        return schema2
    if schema2 is None:
        return schema1

    return update_dict(schema1, schema2)


def update_dict(d1: dict, d2: dict) -> dict:
    """Recursively updated one dictionary with the contents of a second."""
    for k, v in d2.items():
        if isinstance(d1, dict):
            if isinstance(v, dict):
                d1[k] = update_dict(d1.get(k, {}), v)
            elif isinstance(v, list):
                if isinstance(v[0], dict):
                    for i in range(len(v)):
                        d1[k][i] = update_dict(d1.get(k, {})[i], v[i])
                else:
                    d1[k] = list(set(d1.get(k, []) + v))
            else:
                d1[k] = d2[k]
        else:
            d1 = {k: d2[k]}
    return d1


"""Heavy borrowing from git repo anelendata/getschema"""


def _is_datetime(obj):
    """Determine if an object is a datetime or not."""
    # TODO: This is a very loose regex for date-time.
    return (
            type(obj) is datetime.datetime or
            type(obj) is datetime.date or
            (type(obj) is str and re.match("(19|20)\\d\\d-(0[1-9]|1[012])-([1-9]|0[1-9]|[12][0-9]|3[01])", obj) is not None)
    )


def _do_infer_schema(obj):
    """Recursively construct a singer schema based on a provided schema using singer types."""
    if obj is None:
        schema = None
    elif type(obj) is dict and obj.keys():
        object_properties_list = []
        for key in obj.keys():
            ret = _do_infer_schema(obj[key])
            if ret:
                object_properties_list.append(th.Property(key, ret, required=False))

        schema = th.ObjectType(*object_properties_list)

    elif type(obj) is list:
        if len(obj):
            ret = _do_infer_schema(obj[0])
            schema = th.ArrayType(ret)
        else:
            schema = None  # should be th.ArrayType(), but can't be empty
    else:
        try:
            float(obj)
        except (ValueError, TypeError):
            schema = th.StringType
            if _is_datetime(obj):
                schema = th.DateTimeType
        else:
            if type(obj) == bool:
                schema = th.BooleanType
            elif type(obj) == float:  # or (type(obj) == str and "." in obj)
                schema = th.NumberType
            # Let's assume it's a code such as zipcode if there is a leading 0
            elif type(obj) == int:  # or (type(obj) == str and obj[0] != "0")
                schema = th.IntegerType
            else:
                schema = th.StringType
    return schema


def flatten_json(y, except_keys=None):
    out = {}
    if not except_keys:
        except_keys = []

    def t(s):
        translation_table = s.maketrans('-.', '__')
        return s.translate(translation_table)

    def flatten(x, exception_keys, name=''):
        if type(x) is dict:
            for k in x:
                if name + k in exception_keys:
                    out[t(name + k)] = json.dumps(x[k])
                else:
                    flatten(x[k], exception_keys, name + k + '_')

        elif type(x) is list:
            out[t(name[:-1])] = json.dumps(x)

        else:
            out[t(name[:-1])] = x

    flatten(y, exception_keys=except_keys)
    return out
