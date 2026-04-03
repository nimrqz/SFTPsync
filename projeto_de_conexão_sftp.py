import os
import paramiko
import datetime
import time
import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List
from pathlib import Path
import datetime
import sys

# ==============================================================================
# DOCUMENTAÇÂO:
# AUTOR: Nicolas Marcelino
# DATA: 02/04/2026
# DESCRIÇÃO: Script para baixar arquivos de uma pasta via SFTP, com logging detalhado e envio de email ao final.
# ==============================================================================

# ==============================================================================
# CONFIGURAÇÕES GLOBAIS
# ==============================================================================

SMTP_SERVER: str = "***********"
SMTP_PORT: int = 00
EMAIL_SENDER: str = "*******@********"
EMAIL_PASSWORD: str = ""
EMAIL_RECIPIENTS: List[str] = []

# Parâmetros de conexão SFTP
hostname = '**********'
port = 00
username = '****'
password = '******'
timeout = 600

# Caminhos de Diretórios
LOGS_DIRECTORY: str = r""
LOG_FILE_PATH = (Path(LOGS_DIRECTORY) / f"")

# ==============================================================================
# SISTEMA DE LOGGING
# ==============================================================================

def setup_logger(nome: str, caminho: str) -> logging.Logger:
    """Configura o sistema de logging."""
    logger = logging.getLogger(nome)
    logger.setLevel(logging.DEBUG)

    # Limpa handlers existentes para evitar logs duplicados em caso de restart
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Handler para gravar em arquivo (UTF-8 evita erro com caracteres especiais)
    file_handler = logging.FileHandler(caminho, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Handler para exibir no terminal (Stream)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger: logging.Logger = setup_logger("Logger", str(LOG_FILE_PATH))

# ==============================================================================
# COMUNICAÇÃO (E-MAIL)
# ==============================================================================

def send_log_email(subject: str, status: str) -> bool:
    """
    Formata e envia um e-mail com o arquivo de log anexado.
    
    Args:
        subject (str): Assunto do e-mail.
        status (str): Status final da operação (SUCESSO, ERRO, etc).
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = ", ".join(EMAIL_RECIPIENTS)
        msg["Subject"] = subject

        # Corpo do email
        body = f"""
        Automação de extração de arquivos de Sell-in da Iconic executada em {datetime.datetime.now().strftime('(%Y-%m-%d)_(%H:%M:%S)')}
        
        STATUS: {status}
        Arquivo de log: {LOG_FILE_PATH.name}
        
        Verifique o arquivo em anexo para detalhes completos.
        """

        msg.attach(MIMEText(body, "plain", "utf-8"))

        # Anexar arquivo de log
        if LOG_FILE_PATH.exists():
            with open(LOG_FILE_PATH, "rb") as f:
                attachment = MIMEApplication(f.read(), _subtype="txt")
                attachment.add_header("Content-Disposition", "attachment", filename=LOG_FILE_PATH.name)
                msg.attach(attachment)

        # Enviar email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            #server.login(self.config.EMAIL_SENDER, self.config.EMAIL_PASSWORD)

            server.send_message(msg)

        logger.info("E-mail com log enviado com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro ao enviar email: {str(e)}")
        return False

# ==============================================================================
# NÚCLEO DA AUTOMAÇÃO (SFTP)
# ==============================================================================

def run() -> bool:
    """
    Executa o fluxo principal: Conecta, Lista, Baixa arquivos com Retry e Reconexão.
    Retorna True se finalizar sem erros críticos.
    """
    status = "INICIANDO"
    # Definição das pastas dentro da função para evitar erro de escopo
    remote_directory = ''
    local_directory = r''  
    
    # Inicializa cliente SSH
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def conectar_sftp():
        """Helper para abrir conexão e configurar KeepAlive (evita queda por inatividade)."""
        logger.info("Estabelecendo conexão com o servidor...")
        ssh_client.connect(hostname, port, username, password, timeout=timeout)
        transport = ssh_client.get_transport()
        transport.set_keepalive(30)  # Mantém a conexão ativa enviando pings
        return ssh_client.open_sftp()

    try:
        sftp = conectar_sftp()
        
        # Obtém atributos (nome e tamanho) de uma vez só (Otimização de I/O)
        files_attr = sftp.listdir_attr(remote_directory)
        total_files = len(files_attr)
        logger.info(f"Total de arquivos encontrados: {total_files}")

        # Listagem inicial de todos os arquivos e tamanhos
        for attr in files_attr:
            size_mb = attr.st_size / (1024 * 1024)
            logger.info(f"Arquivo na fila: {attr.filename} | Tamanho: {size_mb:.2f} MB")

        files_processed = 0
        now = datetime.datetime.now()
        logger.info("Iniciou processamento em: " + now.strftime("%Y-%m-%d %H:%M:%S"))

        for attr in files_attr:
            f_name = attr.filename
            remote_path = remote_directory + f_name
            local_path = os.path.join(local_directory, f_name)
            
            success = False
            max_attempts = 3

            # Loop de Retry por arquivo
            for attempt in range(1, max_attempts + 1):
                try:
                    # --- VERIFICAÇÃO DE CONEXÃO ---
                    # BLINDAGEM: Verifica se a conexão caiu antes de iniciar o download
                    try:
                        sftp.stat(remote_directory)
                    except Exception:
                        logger.warning(f"Conexão perdida. Tentando reconectar para baixar {f_name}...")
                        sftp = conectar_sftp()

                    file_size_mb = attr.st_size / (1024 * 1024)
                    logger.info(f"Baixando: {f_name} ({file_size_mb:.2f} MB) - Tentativa {attempt}")
                    
                    sftp.get(remote_path, local_path)
                    
                    # Se quiser mover o arquivo após o download, descomente abaixo:
                    remote_file_process = '/Processados/' + f_name
                    sftp.rename(remote_path, remote_file_process)
                    
                    success = True
                    files_processed += 1
                    logger.info(f"Sucesso ao processar: {f_name}")
                    break  # Sai do loop de tentativas para este arquivo

                except Exception as e:
                    logger.error(f"Erro no arquivo {f_name} (Tentativa {attempt}): {str(e)}")
                    if attempt < max_attempts:
                        logger.info("Aguardando 2 segundos para tentar novamente...")
                        time.sleep(2)
                    else:
                        logger.error(f"Falha definitiva no arquivo {f_name} após {max_attempts} tentativas.")

        now = datetime.datetime.now()
        logger.info("Terminou em: " + now.strftime("%Y-%m-%d %H:%M:%S"))
        
        # Define o status final para o e-mail
        if files_processed == total_files:
            status = "SUCESSO"
        elif files_processed > 0:
            status = "SUCESSO_PARCIAL"
        else:
            status = "ERRO"

        logger.info(f"Processamento concluído. Total: {files_processed} de {total_files}")
        
        sftp.close()
        ssh_client.close()
        return True

    except Exception as e:
        logger.critical(f"Erro crítico na automação: {str(e)}")
        status = "ERRO_CRITICO"
        return False

    finally:
        # Garante o envio do e-mail independente do que acontecer no código
        try:
            subject = f"AUTOMAÇÂO DE CONEXÂO SFTP - (STATUS: {status})]"
            logger.info("Aguardando 30s para enviar e-mail de log...")
            time.sleep(30)
            send_log_email(subject, status)
        except Exception as e:
            logger.error(f"Falha ao enviar e-mail de log: {str(e)}")
                
# ==============================================================================
# PONTO DE ENTRADA (MAIN)
# ==============================================================================                

def main() -> None:
    """Gerencia o ciclo de vida da aplicação e códigos de saída para o SO."""
    try:
        # Executar automação
        success = run()

        # Código de saída
        exit_code = 0 if success else 1
        sys.exit(exit_code)

    except Exception as e:
        logger.critical(f"Erro crítico na função main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()