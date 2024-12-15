# storage_node.py

import os
import shutil
import json
import hashlib
from rpyc.utils.server import ThreadedServer
import rpyc

class StorageNode(rpyc.Service):
    def __init__(self, node_id, storage_dir):
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.fragment_index = {}
        self._ensure_storage_dir()

    def _ensure_storage_dir(self):
        """Garante que o diretório de armazenamento exista."""
        os.makedirs(self.storage_dir, exist_ok=True)

    def exposed_store_fragment(self, image_name, fragment_id, data):
        """
        Armazena um fragmento de imagem.

        Args:
            image_name (str): Nome da imagem
            fragment_id (str): ID do fragmento
            data (bytes): Dados do fragmento

        Returns:
            bool: Sucesso da operação
        """
        try:
            file_path = self._get_fragment_path(image_name, fragment_id)
            with open(file_path, 'wb') as f:
                f.write(data)
            
            self._update_fragment_index(image_name, fragment_id, file_path)
            return True
        except Exception as e:
            print(f"Erro ao armazenar fragmento: {e}")
            return False

    def exposed_retrieve_fragment(self, image_name, fragment_id):
        """
        Recupera um fragmento de imagem.

        Args:
            image_name (str): Nome da imagem
            fragment_id (str): ID do fragmento

        Returns:
            bytes: Dados do fragmento ou None se não encontrado
        """
        try:
            file_path = self._get_fragment_path(image_name, fragment_id)
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    return f.read()
            return None
        except Exception as e:
            print(f"Erro ao recuperar fragmento: {e}")
            return None

    def exposed_delete_fragment(self, image_name, fragment_id):
        """
        Deleta um fragmento de imagem.

        Args:
            image_name (str): Nome da imagem
            fragment_id (str): ID do fragmento

        Returns:
            bool: Sucesso da operação
        """
        try:
            file_path = self._get_fragment_path(image_name, fragment_id)
            if os.path.exists(file_path):
                os.remove(file_path)
                self._remove_from_fragment_index(image_name, fragment_id)
                return True
            return False
        except Exception as e:
            print(f"Erro ao deletar fragmento: {e}")
            return False

    def exposed_list_fragments(self):
        """
        Lista todos os fragmentos armazenados neste nó.

        Returns:
            dict: Dicionário de imagens e seus fragmentos
        """
        return self.fragment_index

    def exposed_get_storage_info(self):
        """
        Retorna informações sobre o armazenamento deste nó.

        Returns:
            dict: Informações de armazenamento
        """
        total, used, free = shutil.disk_usage(self.storage_dir)
        return {
            "total_space": total,
            "used_space": used,
            "free_space": free,
            "fragment_count": sum(len(fragments) for fragments in self.fragment_index.values())
        }

    def _get_fragment_path(self, image_name, fragment_id):
        """Gera o caminho do arquivo para um fragmento."""
        return os.path.join(self.storage_dir, f"{image_name}_{fragment_id}")

    def _update_fragment_index(self, image_name, fragment_id, file_path):
        """Atualiza o índice de fragmentos."""
        if image_name not in self.fragment_index:
            self.fragment_index[image_name] = {}
        self.fragment_index[image_name][fragment_id] = file_path
        self._save_fragment_index()

    def _remove_from_fragment_index(self, image_name, fragment_id):
        """Remove um fragmento do índice."""
        if image_name in self.fragment_index:
            self.fragment_index[image_name].pop(fragment_id, None)
            if not self.fragment_index[image_name]:
                self.fragment_index.pop(image_name)
            self._save_fragment_index()

    def _save_fragment_index(self):
        """Salva o índice de fragmentos em um arquivo."""
        index_path = os.path.join(self.storage_dir, 'fragment_index.json')
        with open(index_path, 'w') as f:
            json.dump(self.fragment_index, f)

    def _load_fragment_index(self):
        """Carrega o índice de fragmentos de um arquivo."""
        index_path = os.path.join(self.storage_dir, 'fragment_index.json')
        if os.path.exists(index_path):
            with open(index_path, 'r') as f:
                self.fragment_index = json.load(f)

    def exposed_verify_integrity(self, image_name, fragment_id, expected_hash):
        """
        Verifica a integridade de um fragmento.

        Args:
            image_name (str): Nome da imagem
            fragment_id (str): ID do fragmento
            expected_hash (str): Hash esperado do fragmento

        Returns:
            bool: True se a integridade for verificada, False caso contrário
        """
        fragment_data = self.exposed_retrieve_fragment(image_name, fragment_id)
        if fragment_data:
            actual_hash = hashlib.md5(fragment_data).hexdigest()
            return actual_hash == expected_hash
        return False

def start_storage_node(node_id, storage_dir, port):
    node = StorageNode(node_id, storage_dir)
    server = ThreadedServer(node, port=port)
    print(f"Storage Node {node_id} iniciado na porta {port}")
    server.start()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Uso: python storage_node.py <node_id> <storage_dir> <port>")
        sys.exit(1)
    
    node_id = sys.argv[1]
    storage_dir = sys.argv[2]
    port = int(sys.argv[3])
    
    start_storage_node(node_id, storage_dir, port)
