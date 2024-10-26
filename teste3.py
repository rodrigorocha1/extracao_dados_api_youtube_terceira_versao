from abc import ABC, abstractmethod

# Classe abstrata Funcionario


class Funcionario(ABC):
    def __init__(self, nome: str, salario: float):
        self.nome = nome
        self.salario = salario

    # Método abstrato que deve ser implementado pelas subclasses
    @abstractmethod
    def calcular_bonus(self) -> float:
        pass

# Classe Vendedor, herda de Funcionario


class Vendedor(Funcionario):
    def __init__(self, nome: str, salario: float, vendas: float):
        super().__init__(nome, salario)
        self.vendas = vendas

    # Implementação do método abstrato
    def calcular_bonus(self) -> float:
        # Exemplo: o bônus do vendedor é 5% do total de vendas
        return self.vendas * 0.05

# Classe principal MainBase que trabalha com objetos de funcionários


class MainBase:
    def __init__(self, funcionario: Funcionario):  # Tipo anotado
        # Garantindo que seja um Funcionario
        self.funcionario = funcionario

    def exibir_informacoes(self):
        print(f"Nome: {self.funcionario.nome}")
        print(f"Salário: {self.funcionario.salario}")
        print(f"Bônus: {self.funcionario.calcular_bonus()}")


# Criando um objeto Vendedor
vendedor = Vendedor("João", 3000, 50000)

# Criando a base com o objeto Vendedor
main_base = MainBase(vendedor)

# Exibindo as informações do vendedor
print(type(vendedor))
print(isinstance(vendedor, Funcionario))
