import json
import warnings

from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, List, Union

import numpy
import pandas
import uproot
from dateutil import parser

from utils import Singleton
from utils.logging_utils import UtilsLogger


class JSONDeserializeError(OSError, ValueError):
    pass


class JSONSerializer(ABC, Singleton):
    """Abstract Base Class for preserving type between 
    writing and reading the JSON file with an intermediate 
    string representation"""
    @abstractmethod
    def serialize(self, obj: Any) -> str:
        pass

    @abstractmethod
    def deserialize(self, r_value: str) -> Any:
        pass


class JSONConverter(ABC, Singleton):
    """Abstract Base Class for converting a non-python type to a python
    type when writing a JSON File, type will not be preserved"""
    @abstractmethod
    def convert(self, obj: Any) -> Any:
        pass


JSONHandler = Union[JSONSerializer, JSONConverter]


class DatetimeJSON(JSONSerializer):
    def __init__(self):
        self.name = "datetime"
        self.types = [datetime, date]

    def serialize(self, obj):
        return obj.isoformat()

    def deserialize(self, r_value):
        return parser.parse(r_value)


class TimedeltaJSON(JSONSerializer):
    def __init__(self):
        self.name = "timedelta"
        self.types = [timedelta]

    def serialize(self, obj):
        return f"{str(obj.days)},{str(obj.seconds)},{str(obj.microseconds)}"

    def deserialize(self, r_value):
        return timedelta(*tuple(map(lambda xx: int(xx), r_value.split(","))))

class PdDataframe(JSONSerializer):
    def __init__(self):
        self.name = "pd_dataframe"
        self.types = [pandas.DataFrame]

    def serialize(self, obj):
        return obj.to_json()

    def deserialize(self, r_value):
        return pandas.read_json(r_value)

class NDArrayJSON(JSONConverter):
    def __init__(self):
        self.name = "ndarray"
        self.types = [numpy.ndarray]

    def convert(self, obj):
        return obj.tolist()

class NumpyFloat(JSONConverter):
    def __init__(self):
        self.name = "np_float"
        self.types = [numpy.float128, numpy.float64,
                      numpy.float32, numpy.float16]

    def convert(self, obj):
        return float(obj)


class NumpyInt(JSONConverter):
    def __init__(self):
        self.name = "np_int"
        self.types = [numpy.int8, numpy.uint8, numpy.int32, numpy.int64,
                      numpy.uint32, numpy.int16, numpy.uint16, numpy.uint64]

    def convert(self, obj):
        return int(obj)


class UprootContainer(JSONConverter):
    def __init__(self):
        self.name = "uproot_stlc"
        self.types = [
            uproot.containers.STLVector,
            uproot.containers.STLSet, 
            uproot.containers.STLMap
        ]

    def convert(self, obj):
        return obj.tolist()


class JSONMixin:
    """
    A mix-in class to build other JSON serializing/deserializing
    classes to enable using non-standard types inside JSON
    """
    names_to_serializers: dict[str, JSONSerializer] = {
        "datetime": DatetimeJSON(),
        "timedelta": TimedeltaJSON(),
        "pd_dataframe": PdDataframe(),
    }

    names_to_converters: dict[str, JSONConverter] = {
        "ndarray": NDArrayJSON(),
        "np_float": NumpyFloat(),
        "np_int": NumpyInt(),
        "uproot_stlc": UprootContainer()
    }

    def __init__(self):
        self.types_to_names: dict[str, JSONHandler] = {}
        for type_c in (self.names_to_converters, self.names_to_serializers):
            for name, type_handler in type_c.items():
                for d_type in type_handler.types:
                    self.types_to_names[d_type] = name

    def json_serial(self, obj):
        """JSON serializer for objects which by default json are not serializable
        using either the converters which replaces the object with one that is understood
        by json or the serializers which manually converts it into an intermediate format
        as represented by a dictionary"""
        con_name = self.types_to_names[type(obj)]
        if con_name in self.names_to_converters:
            return self.names_to_converters[con_name].convert(obj)
        elif con_name in self.names_to_serializers:
            serial_str = self.names_to_serializers[con_name].serialize(obj)
            return {
                "__type__": con_name,
                "repr": serial_str
            }
        else:
            raise TypeError(f"Type {type(obj)} not serializable") from e

    def json_deserial(self, obj):
        """JSON Deserializer for objects identified as
        being serialized by this mix-in class using the serializers defined
        in the class variable names_to_serializers"""

        if isinstance(obj, dict):
            for recovered_key, recovered_value in obj.items():
                if isinstance(recovered_value, dict) and ("__type__" in recovered_value):
                    recover_type = recovered_value.pop("__type__")
                    recover_repr = recovered_value.pop("repr")
                    try:
                        serializer: JSONSerializer = self.names_to_serializers[recover_type]
                        obj[recovered_key] = \
                            serializer.deserialize(recover_repr)
                    except KeyError as e:
                        raise TypeError(
                            f"Unable to parse JSON Serialized Object {recover_type} with representation {recover_repr}") from e

        return obj

    def _set_kwargs(self, kwargs, default_kwargs, override):
        for dkw_arg, dkw_val in default_kwargs.items():
            if dkw_arg in kwargs:
                if override:
                    continue
                else:
                    warnings.warn(
                        f"Overriding default keyword {str(dkw_arg)}"
                        + f" used by {type(self)} without specifying to override!",
                        UserWarning,
                    )

            kwargs[dkw_arg] = dkw_val

    def to_json_str(self, in_obj: Any, override_kwargs=False, **kwargs) -> str:
        self._set_kwargs(
            kwargs, {"indent": 2, "default": self.json_serial}, override_kwargs
        )
        return json.dumps(in_obj, **kwargs)

    def from_json_str(self, in_str: str, override_kwargs=False, **kwargs) -> Any:
        if not in_str:
            raise ValueError("No input to deserialize!")
        self._set_kwargs(
            kwargs, {"object_hook": self.json_deserial}, override_kwargs)
        return json.loads(in_str, **kwargs)

    def dump_to_json_file(
            self, in_obj: dict, in_file: Path, override_kwargs=False, **kwargs
    ) -> None:
        self._set_kwargs(
            kwargs, {"indent": 2, "default": self.json_serial}, override_kwargs
        )
        fp = in_file.open("w")
        json.dump(in_obj, fp, **kwargs)
        fp.close()

    def load_json_file(self, in_filepath: Union[str, Path], override_kwargs=False, **kwargs) -> Any:
        if not isinstance(in_filepath, Path):
            in_filepath = Path(in_filepath)
        if not in_filepath.exists():
            return {}
        try:
            return self.from_json_str(in_filepath.read_text())
        except (OSError, ValueError) as err:
            raise JSONDeserializeError(
                f"Unable to deserialize file {in_filepath}") from err

    def update_json_file(
            self,
            in_filepath: Union[str, Path],
            in_data: dict,
            override_kwargs=False,
            **kwargs,
    ):
        if not isinstance(in_filepath, Path):
            in_filepath = Path(in_filepath)

        in_filepath.parent.mkdir(parents=True, exist_ok=True)

        file_data = self.load_json_file(in_filepath)
        file_data.update(in_data)
        if "debug" in kwargs:
            UtilsLogger.debug(f"Writing to {in_filepath}")
        self.dump_to_json_file(
            file_data, in_filepath, override_kwargs=override_kwargs, **kwargs
        )
        if "debug" in kwargs:
            UtilsLogger.debug(f"Finished writing to {in_filepath}")

    def get_list(self, in_list, key_mangler=None, return_type=dict, return_mode=2):
        """
        Returns the data from current file with keys inside in_list

        Parameters
        ----------
        in_list - Key list
        key_mangler - A function to alter the keys before using them to search
        return_type - What type of object to return
        ignore_not_found - Do not return keys which were not found in the data

        Returns
        -------
        The requested data

        """
        cur_data = self.load_data()
        rv = return_type()
        not_found_rv = []
        for ele in in_list:
            if key_mangler is not None:
                ele = key_mangler(ele)

            if ele in cur_data:
                got_data = cur_data[ele]
            else:
                not_found_rv.append(ele)
                continue

            if isinstance(rv, dict):
                rv[ele] = got_data
            elif isinstance(rv, list):
                rv.append(got_data)
            elif isinstance(rv, set):
                rv.add(got_data)
            else:
                raise TypeError(f"Unknown return type {type(rv)} in get_list!")

        if return_mode == 0:
            return rv
        elif return_mode == 1:
            return not_found_rv
        else:
            return rv, not_found_rv

    def load_data(self, *args, **kwargs):
        raise NotImplementedError

    def save_data(self, *args, **kwargs):
        raise NotImplementedError


class SimpleJSON(JSONMixin):
    def __init__(self, filepath: Path):
        super().__init__()
        self.file = filepath

    def exists(self):
        return self.file.exists()

    def load_data(self, use_pandas: bool = False, **json_kwargs):
        all_data = self.load_json_file(self.file, **json_kwargs)
        if not use_pandas:
            return all_data.copy()
        if isinstance(all_data, dict):
            return pandas.DataFrame.from_dict(all_data, orient="index")
        else:
            return pandas.DataFrame.from_records(all_data)

    def save_data(
            self,
            data: Union[dict, List],
            sort_fn: Union[Callable, None] = None,
            **json_kwargs,
    ):
        if not callable(sort_fn) and sort_fn is not None:
            raise TypeError("Sort function not callable!")

        if sort_fn is None:
            sorted_data = data
        elif isinstance(data, dict):
            key_list = list(data.keys())
            key_list = sorted(key_list, key=sort_fn)
            sorted_data = {key: data[key] for key in key_list}
        elif isinstance(data, (list, tuple)):
            sorted_data = sorted(data, key=sort_fn)
        else:
            raise TypeError(
                "Sorting is only allowed with lists or dictionaries!")
        self.update_json_file(self.file, sorted_data, **json_kwargs)