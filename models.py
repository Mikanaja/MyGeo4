class Models:
    def __init__(self):
        """
        Inicializa as estruturas de dados para gerenciamento de imagens
        
        Estruturas:
        - nos_fragmentos: Mapeia imagens para seus fragmentos e seus respectivos nós
        - tamanho_fragmentos: Armazena o tamanho de cada fragmento
        - tamanho_imagem: Registra o tamanho total de cada imagem
        """
        self.nos_fragmentos = {}      # {nome_imagem: [lista de fragmentos]}
        self.tamanho_fragmentos = {}  # {nome_imagem: [tamanhos dos fragmentos]}
        self.tamanho_imagem = {}      # {nome_imagem: tamanho total}

    def iniciar_imagem(self, nome_imagem, divisoes):
        """
        Inicializa entrada para uma nova imagem no sistema

        Args:
            nome_imagem (str): Nome da imagem
            divisoes (int): Número de fragmentos que a imagem será dividida

        Returns:
            bool: True se a imagem foi inicializada, False se já existir
        """
        if nome_imagem in self.nos_fragmentos:
            return False

        # Inicializa as estruturas para a nova imagem
        self.nos_fragmentos[nome_imagem] = [[] for _ in range(divisoes)]
        self.tamanho_fragmentos[nome_imagem] = [None for _ in range(divisoes)]
        self.tamanho_imagem[nome_imagem] = 0

        return True

    def atualizar_fragmento(self, nome_imagem, indice, tamanho, nos):
        """
        Atualiza as informações de um fragmento de imagem

        Args:
            nome_imagem (str): Nome da imagem
            indice (int): Índice do fragmento
            tamanho (int): Tamanho do fragmento
            nos (list): Lista de nós que armazenam o fragmento
        """
        # Adiciona os nós ao fragmento se ainda não existirem
        for no in nos:
            if no not in self.nos_fragmentos[nome_imagem][indice]:
                self.nos_fragmentos[nome_imagem][indice].append(no)

        # Atualiza o tamanho do fragmento
        self.tamanho_fragmentos[nome_imagem][indice] = tamanho

        # Atualiza o tamanho total da imagem
        self.tamanho_imagem[nome_imagem] += tamanho

    def obter_fragmentos(self, nome_imagem):
        """
        Retorna uma lista de fragmentos de uma imagem

        Args:
            nome_imagem (str): Nome da imagem

        Returns:
            list: Lista de dicionários com informações dos fragmentos
        """
        if nome_imagem not in self.nos_fragmentos:
            return []

        return [
            {
                'nos': self.nos_fragmentos[nome_imagem][indice],
                'tamanho': self.tamanho_fragmentos[nome_imagem][indice]
            }
            for indice in range(len(self.nos_fragmentos[nome_imagem]))
        ]

    def remover_imagem(self, nome_imagem):
        """
        Remove uma imagem do sistema

        Args:
            nome_imagem (str): Nome da imagem a ser removida

        Returns:
            bool: True se a imagem foi removida, False se não existir
        """
        if nome_imagem not in self.nos_fragmentos:
            return False

        # Remove a imagem de todas as estruturas
        del self.nos_fragmentos[nome_imagem]
        del self.tamanho_fragmentos[nome_imagem]
        del self.tamanho_imagem[nome_imagem]

        return True

    def listar_imagens(self):
        """
        Lista todas as imagens no sistema

        Returns:
            list: Nomes das imagens armazenadas
        """
        return list(self.nos_fragmentos.keys())

    def obter_tamanho_imagem(self, nome_imagem):
        """
        Obtém o tamanho total de uma imagem

        Args:
            nome_imagem (str): Nome da imagem

        Returns:
            int: Tamanho total da imagem, ou 0 se não existir
        """
        return self.tamanho_imagem.get(nome_imagem, 0)

    def obter_total_fragmentos(self, nome_imagem):
        """
        Obtém o número total de fragmentos de uma imagem

        Args:
            nome_imagem (str): Nome da imagem

        Returns:
            int: Número de fragmentos, ou 0 se não existir
        """
        if nome_imagem not in self.nos_fragmentos:
            return 0
        return len(self.nos_fragmentos[nome_imagem])

    def verificar_imagem_completa(self, nome_imagem):
        """
        Verifica se todos os fragmentos de uma imagem têm tamanho definido

        Args:
            nome_imagem (str): Nome da imagem

        Returns:
            bool: True se todos os fragmentos estão completos, False caso contrário
        """
        if nome_imagem not in self.tamanho_fragmentos:
            return False

        return all(tamanho is not None for tamanho in self.tamanho_fragmentos[nome_imagem])

    def __repr__(self):
        """
        Representação em string do modelo

        Returns:
            str: Descrição das imagens armazenadas
        """
        return f"Modelo de Imagens: {len(self.nos_fragmentos)} imagens armazenadas"


# Exemplo de uso
def exemplo_uso():
    # Criando uma instância do modelo
    modelo = Models()

    # Iniciando uma nova imagem
    modelo.iniciar_imagem("satelite.jpg", divisoes=3)

    # Atualizando fragmentos
    modelo.atualizar_fragmento("satelite.jpg", 0, 1024, ["node1", "node2"])
    modelo.atualizar_fragmento("satelite.jpg", 1, 2048, ["node3", "node4"])
    modelo.atualizar_fragmento("satelite.jpg", 2, 1536, ["node1", "node5"])

    # Listando imagens
    print(modelo.listar_imagens())

    # Obtendo fragmentos
    fragmentos = modelo.obter_fragmentos("satelite.jpg")
    for i, fragmento in enumerate(fragmentos):
        print(f"Fragmento {i}: {fragmento}")

    # Verificando tamanho total
    print(f"Tamanho total: {modelo.obter_tamanho_imagem('satelite.jpg')} bytes")

    # Verificando completude da imagem
    print(f"Imagem completa: {modelo.verificar_imagem_completa('satelite.jpg')}")