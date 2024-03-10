
from abc import abstractmethod
from typing import TypeVar

TApplication = TypeVar('TApplication') 


class BaseApplication[TApplication]:
    
    @abstractmethod
    def start(self)-> None:
        pass
    