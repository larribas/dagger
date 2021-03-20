from abc import ABC, abstractmethod


class Serializer(ABC):
    @property
    @abstractmethod
    def extension(self):
        raise NotImplementedError


class JSON(Serializer):
    @property
    def extension(self):
        return "json"
