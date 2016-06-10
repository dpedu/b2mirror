from enum import Enum
from collections import namedtuple


FileInfo = namedtuple('FileInfo', ['abs_path', 'rel_path', 'size', 'mtime', ])  # 'fp'


class Result(Enum):
    failed = 0
    ok = 1
    skipped = 2


results_ok = [Result.ok, Result.skipped]
