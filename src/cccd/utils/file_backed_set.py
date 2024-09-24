import pickle
import os

df_path_template = "work/{name}/bins/{set_name}.bin"

class FileBackedSet:
    def __init__(self, run_name, set_name) -> None:
        self.file_path = df_path_template.replace("{name}", run_name).replace("{set_name}", set_name)

        if not os.path.exists(self.file_path):
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            self._persist(self.file_path, set())

        self.collection = self._load(self.file_path)

    def add(self, value: str, persist=True) -> None:
        self.collection.add(value)

        if persist:
            self._persist(self.file_path, self.collection)
    
    def close(self):
        self._persist(self.file_path, self.collection)

    def __contains__(self, key):
        return key in self.collection
    
    @staticmethod
    def _persist(file_path, collection):
        with open(file_path, "wb") as f:
            pickle.dump(collection, f)

    @staticmethod
    def _load(file_path):
        with open(file_path, "rb") as f:
            loaded = pickle.load(f)

        assert isinstance(loaded, set)
        return loaded
