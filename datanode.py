import os
import rpyc
import time
import random
from rpyc.utils.server import ThreadedServer

class DataNode(rpyc.Service):
    DIRETORIO_ARMAZENAMENTO = 'datanode_storage'

    def __init__(self, porta=8001):
        self.porta = porta
        
        # Cria diretório de armazenamento se não existir
        os.makedirs(self.DIRETORIO_ARMAZENAMENTO, exist_ok=True)
        
        # Configurações de armazenamento
        self.max_storage_mb = 1024  # 1 GB
        self.current_storage_mb = 0

    def _gerar_caminho_fragmento(self, nome_imagem, indice_fragmento):
        """
        Gera caminho para armazenar fragmento
        """
        return os.path.join(
            self.DIRETORIO_ARMAZENAMENTO, 
            f'{nome_imagem}_parte_{indice_fragmento}'
        )

    def exposed_obter_status_no(self):
        """
        Retorna status do nó (simulado)
        """
        return {
            'cpu': random.uniform(10, 80),      # Uso de CPU
            'memoria': random.uniform(20, 90),  # Uso de memória
            'disco': (self.current_storage_mb / self.max_storage_mb) * 100  # Uso de disco
        }

    def exposed_armazenar_fragmento_imagem(self, nome_imagem, indice_fragmento, fragmento):
        """
        Armazena um fragmento de imagem
        """
        try:
            # Verifica espaço em disco
            fragmento_size_mb = len(fragmento) / (1024 * 1024)
            
            if self.current_storage_mb + fragmento_size_mb > self.max_storage_mb:
                print("[ERRO] Espaço em disco insuficiente")
                return False

            caminho = self._gerar_caminho_fragmento(nome_imagem, indice_fragmento)
            
            with open(caminho, 'ab') as arquivo:
                arquivo.write(fragmento)
            
            # Atualiza uso de disco
            self.current_storage_mb += fragmento_size_mb
            
            print(f"[STATUS] Fragmento {nome_imagem}_parte_{indice_fragmento} armazenado")
            return True

        except Exception as e:
            print(f"[ERRO] Falha ao armazenar fragmento: {e}")
            return False

    def exposed_recuperar_fragmento_imagem(self, nome_imagem, indice_fragmento):
        """
        Recupera um fragmento de imagem
        
        Returns:
            tuple: (fragmento, fim_leitura)
        """
        try:
            caminho = self._gerar_caminho_fragmento(nome_imagem, indice_fragmento)
            
            if not os.path.exists(caminho):
                print(f"[ERRO] Fragmento {nome_imagem}_parte_{indice_fragmento} não encontrado")
                return None, False

            with open(caminho, 'rb') as arquivo:
                fragmento = arquivo.read()
            
            return fragmento, True

        except Exception as e:
            print(f"[ERRO] Falha ao recuperar fragmento: {e}")
            return None, False

    def exposed_deletar_fragmento_imagem(self, nome_imagem, indice_fragmento):
        """
        Deleta um fragmento de imagem
        """
        try:
            caminho = self._gerar_caminho_fragmento(nome_imagem, indice_fragmento)
            
            if os.path.exists(caminho):
                # Calcula tamanho do arquivo para atualizar uso de disco
                fragmento_size_mb = os.path.getsize(caminho) / (1024 * 1024)
                
                os.remove(caminho)
                
                # Atualiza uso de disco
                self.current_storage_mb -= fragmento_size_mb
                
                print(f"[STATUS] Fragmento {nome_imagem}_parte_{indice_fragmento} deletado")
                return True
            else:
                print(f"[AVISO] Fragmento {nome_imagem}_parte_{indice_fragmento} não encontrado")
                return False

        except Exception as e:
            print(f"[ERRO] Falha ao deletar fragmento: {e}")
            return False

    def exposed_listar_fragmentos(self):
        """
        Lista todos os fragmentos armazenados
        """
        try:
            fragmentos = [
                arquivo for arquivo in os.listdir(self.DIRETORIO_ARMAZENAMENTO)
                if os.path.isfile(os.path.join(self.DIRETORIO_ARMAZENAMENTO, arquivo))
            ]
            return fragmentos
        except Exception as e:
            print(f"[ERRO] Falha ao listar fragmentos: {e}")
            return []

def iniciar_datanode(porta=8001):
    """
    Inicia o servidor do DataNode
    """
    try:
        print(f"[STATUS] Iniciando DataNode na porta {porta}")
        
        # Configurações do servidor
        servidor = ThreadedServer(
            DataNode, 
            port=porta, 
            protocol_config={
                'allow_public_attrs': True,
                'sync_request_timeout': 30
            }
        )
        
        print(f"[STATUS] DataNode pronto na porta {porta}")
        servidor.start()
    
    except Exception as e:
        print(f"[ERRO] Falha ao iniciar DataNode: {e}")

if __name__ == "__main__":
    import sys
    
    # Permite especificar porta via linha de comando
    porta = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    
    iniciar_datanode(porta)