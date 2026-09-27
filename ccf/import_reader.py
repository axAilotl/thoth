"""Read export carriers without extracting archives or following symlinks."""

from pathlib import Path, PurePosixPath
import stat
import zipfile


class ExportReader:
    def __init__(self, source):
        self.path = Path(source)
        self.archive = None

    def __enter__(self):
        if not self.path.is_dir():
            self.archive = zipfile.ZipFile(self.path)
            names = self.archive.namelist()
            if len(names) != len(set(names)):
                self.archive.close()
                raise ValueError("duplicate ZIP member names")
        return self

    def __exit__(self, *args):
        if self.archive is not None:
            self.archive.close()

    def exists(self, name):
        if self.archive is not None:
            return name in self.archive.namelist()
        return (self.path / name).exists()

    def _open(self, name):
        parts = PurePosixPath(name)
        if parts.is_absolute() or ".." in parts.parts or "\\" in name:
            raise ValueError(f"unsafe export member: {name}")
        if self.archive is not None:
            info = self.archive.getinfo(name)
            if stat.S_ISLNK(info.external_attr >> 16):
                raise ValueError(f"symlink export member: {name}")
            return self.archive.open(info)
        path = self.path / name
        if path.is_symlink() or not path.resolve().is_relative_to(self.path.resolve()):
            raise ValueError(f"unsafe export path: {name}")
        return path.open("rb")

    def read(self, name):
        with self._open(name) as stream:
            return stream.read()

    def lines(self, name):
        with self._open(name) as stream:
            yield from stream

    def file_names(self):
        if self.archive is not None:
            return [name for name in self.archive.namelist() if name.startswith("files/") and not name.endswith("/")]
        directory = self.path / "files"
        if not directory.exists():
            return []
        if directory.is_symlink():
            raise ValueError("files directory must not be a symlink")
        return ["files/" + path.name for path in sorted(directory.iterdir())]
