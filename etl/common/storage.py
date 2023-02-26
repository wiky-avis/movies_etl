import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        try:
            with open(self.file_path, "w", encoding="utf-8") as outfile:
                json.dump(state, outfile, ensure_ascii=False, indent=4)
        except FileNotFoundError:
            return

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path) as json_file:
                try:
                    data = json.load(json_file)
                except Exception:
                    return {}
                return data
        except FileNotFoundError:
            return {}


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        data = self.storage.retrieve_state()
        if isinstance(data, dict):
            data.update({key: str(value)})
        self.storage.save_state(data)

    def get_state(self, key: str) -> Any:
        data = self.storage.retrieve_state()
        value = data.get(key)
        return value if value else None
