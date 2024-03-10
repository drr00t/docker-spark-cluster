
from abc import ABC, abstractmethod


class BaseApplication(ABC):
    
    @abstractmethod
    def start(self)-> None:
        pass
    