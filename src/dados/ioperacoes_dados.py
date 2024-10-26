from typing import Generic, TypeVar
from abc import ABC, abstractmethod


T = TypeVar('T')


class IoperacaoDados(ABC, Generic[T]):
    @abstractmethod
    def salvar_dados(self, dados: T) -> None:
        pass

    @abstractmethod
    def carregar_dados(self) -> T:
        pass
