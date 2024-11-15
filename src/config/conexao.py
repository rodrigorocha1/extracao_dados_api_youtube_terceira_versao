from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
load_dotenv()


class ConexaoBancoHive:

    def __init__(self):
        self.__host = os.environ['HOST_HIVE']
        self.__port = os.environ['PORT_HIVE']
        self.__database = os.environ['DATABASE_HIVE']
        self.__url_banco = f'hive://{self.__host}:{self.__port}/{self.__database}'
        self.__conexao = create_engine(self.__url_banco)
        self.__Sessao = sessionmaker(bind=self.__conexao)

    def obter_conexao(self):
        return self.__conexao

    def obter_sessao(self):
        return self.__Sessao()
