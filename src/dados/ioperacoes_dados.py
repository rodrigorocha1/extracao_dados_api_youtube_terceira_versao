from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Dict, List, Any

T = TypeVar('T')


class IoperacaoDados(ABC):
    @abstractmethod
    def salvar_dados(self, dados: T) -> None:
        pass

    @abstractmethod
    def carregar_dados(self) -> T:
        pass


class Arquivo(IoperacaoDados, Generic[T]):
    def __init__(self):
        pass

    @abstractmethod
    def salvar_dados(self) -> None:
        pass

    @abstractmethod
    def carregar_dados(self) -> T:
        pass


class ArquivoJson(Arquivo[Dict[str, Any]]):
    def __init__(self):
        super().__init__()

    def salvar_dados(self) -> None:
        print('Salvando Arquivo JSON')

    def carregar_dados(self) -> Dict[str, Any]:
        # Aqui você pode implementar a lógica de carregar dados de um arquivo JSON.
        # Retornando um dicionário vazio como exemplo.
        return {}


class ArquivoPicke(Arquivo[List[Any]]):
    def __init__(self):
        super().__init__()

    def salvar_dados(self) -> None:
        print('Salvando Arquivo PKL')

    def carregar_dados(self) -> List[Any]:
        # Aqui você pode implementar a lógica de carregar dados de um arquivo PKL.
        # Retornando uma lista vazia como exemplo.
        return []


# Exemplo de uso
a = ArquivoPicke()
print(isinstance(a, Arquivo))
