
from abc import abstractmethod


class Config:
    
    @abstractmethod
    def get_val(self) -> None:
        pass
    