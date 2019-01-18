import msgpack
import struct
from dashsink_utils.ExtensionType import ExtensionType

# TODO Exception Catch
# TODO add test
class MessagePackDocBuilder:

    def __init__(self):
        self.__timestamp = ''
        self.__raw = ''
        self.__dic = dict()
        self.packer = msgpack.Packer(use_bin_type=True, autoreset=False)

    def set_timestamp(self, timestamp):
        timestamp = int(timestamp)
        self.__timestamp = timestamp
        return self

    def get_timestamp(self):
        return self.__timestamp

    def set_raw(self, raw):
        self.__raw = str(raw)
        return self

    def get_raw(self):
        return self.__raw

    def reset(self):
        self.__timestamp = 0
        self.__raw = ''
        self.__dic = dict()
        self.packer.reset()
        return self

    def put_text(self, key, value):
        value = str(value)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_text_offset(self, key, start, end):
        value = self.offset_to_value(ExtensionType.TextOffset, start, end)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_sorted(self, key, value):
        value = msgpack.ExtType(ExtensionType.Sorted.value, self.value_to_bytes(value))
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_sorted_offset(self, key, start, end):
        value = self.offset_to_value(ExtensionType.SortedOffset, start, end)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_lat_lon(self, key, lat, lon):
        value = self.lat_lon_to_value(ExtensionType.LatLon, lat, lon)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_keyword(self, key, value):
        value = msgpack.ExtType(ExtensionType.Keyword.value, self.value_to_bytes(value))
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_int(self, key, value):
        value = int(value)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_long(self, key, value):
        value = int(value)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_float(self, key, value):
        # It is precise, same as double in JAVA
        value = float(value)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = float(value)
        return self

    def put_double(self, key, value):
        # There is no double type in python, use float() here
        value = float(value)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = float(value)
        return self

    def build(self):
        self.packer.reset()
        self.packer.pack(self.__timestamp)
        self.packer.pack(self.__raw)
        for key in self.__dic.keys():
            self.packer.pack(key)
            value = self.__dic[key]
            if isinstance(value, list):
                self.packer.pack_array_header(len(value))
                for v in value:
                    self.packer.pack(v)
            else:
                self.packer.pack(value)
        return self.packer.bytes()

    @staticmethod
    def offset_to_value(ext: ExtensionType, start, end):
        bytes_array = struct.pack('>ii', start, end)
        return msgpack.ExtType(ext.value, bytes_array)

    @staticmethod
    def lat_lon_to_value(ext: ExtensionType, lat, lon):
        bytes_array = struct.pack('>dd', lat, lon)
        return msgpack.ExtType(ext.value, bytes_array)

    @staticmethod
    def value_to_bytes(value):
        if isinstance(value, bytes):
            return value
        return str(value).encode()

    @staticmethod
    def add_multi_value(pre_value, value):
        if isinstance(pre_value, list):
            pre_value.append(value)
            return pre_value
        value_list = [pre_value, value]
        return value_list
