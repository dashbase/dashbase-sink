from enum import Enum, unique

@unique
class ExtensionType(Enum):
    Sorted = 0
    Keyword = 1
    TextOffset = 2
    SortedOffset = 3
    LatLon = 4