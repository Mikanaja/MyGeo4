class ErroUpload(Exception):
    """Erro durante upload de imagem"""
    def __init__(self, mensagem="Erro no upload da imagem", 
                 nome_imagem=None, 
                 causa=None):
        self.mensagem = mensagem
        self.nome_imagem = nome_imagem
        self.causa = causa
        super().__init__(self.mensagem_detalhada())

    def mensagem_detalhada(self):
        detalhes = self.mensagem
        if self.nome_imagem:
            detalhes += f" - Imagem: {self.nome_imagem}"
        if self.causa:
            detalhes += f" - Causa: {str(self.causa)}"
        return detalhes


class ErroDownload(Exception):
    """Erro durante download de imagem"""
    def __init__(self, mensagem="Erro no download da imagem", 
                 nome_imagem=None, 
                 fragmento_atual=None, 
                 causa=None):
        self.mensagem = mensagem
        self.nome_imagem = nome_imagem
        self.fragmento_atual = fragmento_atual
        self.causa = causa
        super().__init__(self.mensagem_detalhada())

    def mensagem_detalhada(self):
        detalhes = self.mensagem
        if self.nome_imagem:
            detalhes += f" - Imagem: {self.nome_imagem}"
        if self.fragmento_atual is not None:
            detalhes += f" - Fragmento: {self.fragmento_atual}"
        if self.causa:
            detalhes += f" - Causa: {str(self.causa)}"
        return detalhes


class ErroConexao(Exception):
    """Erro de conexão com cluster"""
    def __init__(self, mensagem="Erro de conexão com o cluster", 
                 endereco=None, 
                 porta=None, 
                 causa=None):
        self.mensagem = mensagem
        self.endereco = endereco
        self.porta = porta
        self.causa = causa
        super().__init__(self.mensagem_detalhada())

    def mensagem_detalhada(self):
        detalhes = self.mensagem
        if self.endereco:
            detalhes += f" - Endereço: {self.endereco}"
        if self.porta:
            detalhes += f" - Porta: {self.porta}"
        if self.causa:
            detalhes += f" - Causa: {str(self.causa)}"
        return detalhes


class ErroDeletarImagem(Exception):
    """Erro ao deletar imagem"""
    def __init__(self, mensagem="Erro ao deletar imagem", 
                 nome_imagem=None, 
                 causa=None):
        self.mensagem = mensagem
        self.nome_imagem = nome_imagem
        self.causa = causa
        super().__init__(self.mensagem_detalhada())

    def mensagem_detalhada(self):
        detalhes = self.mensagem
        if self.nome_imagem:
            detalhes += f" - Imagem: {self.nome_imagem}"
        if self.causa:
            detalhes += f" - Causa: {str(self.causa)}"
        return detalhes


# Exceções adicionais específicas para o sistema
class ErroTamanhoImagem(ErroUpload):
    """Erro quando o tamanho da imagem excede o limite"""
    def __init__(self, tamanho_maximo, tamanho_atual):
        super().__init__(
            mensagem="Tamanho da imagem excede o limite permitido",
            causa=f"Tamanho máximo: {tamanho_maximo}, Tamanho atual: {tamanho_atual}"
        )


class ErroFragmentacaoImagem(ErroUpload):
    """Erro durante a fragmentação da imagem"""
    def __init__(self, nome_imagem, total_fragmentos, fragmentos_processados):
        super().__init__(
            mensagem="Erro na fragmentação da imagem",
            nome_imagem=nome_imagem,
            causa=f"Total fragmentos: {total_fragmentos}, Fragmentos processados: {fragmentos_processados}"
        )


class ErroRecuperacaoImagem(ErroDownload):
    """Erro na recuperação de fragmentos de imagem"""
    def __init__(self, nome_imagem, fragmentos_faltantes):
        super().__init__(
            mensagem="Falha na recuperação completa da imagem",
            nome_imagem=nome_imagem,
            causa=f"Fragmentos não recuperados: {fragmentos_faltantes}"
        )


# Exemplo de uso
def exemplo_uso():
    try:
        # Simulação de um erro de upload
        raise ErroUpload(
            nome_imagem="satelite.jpg", 
            causa="Falha de permissão"
        )
    except ErroUpload as e:
        print(f"Erro de Upload: {e}")
        # Log do erro
        # Tratamento específico

    try:
        # Simulação de erro de conexão
        raise ErroConexao(
            endereco="192.168.1.100", 
            porta=8000, 
            causa="Timeout de conexão"
        )
    except ErroConexao as e:
        print(f"Erro de Conexão: {e}")
        # Tentar reconexão
        # Log do erro

# Opcional: Adicionar método de log
def log_erro(excecao):
    """
    Método para registrar erros em um arquivo de log
    """
    import logging
    
    logging.basicConfig(
        filename='sistema_erro.log', 
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.error(str(excecao))

# Se quiser usar o log
# log_erro(excecao)