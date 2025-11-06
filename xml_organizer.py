"""
XML Organizer - Versão Ultra-Confiável
======================================

Sistema com garantia de processamento de 100% dos XMLs com múltiplas camadas
de proteção, auditoria completa e recovery automático.

Principais melhorias de confiabilidade:
- Quarentena antes de processar (staging area)
- Retry automático com backoff exponencial
- Transações atômicas (BD + movimentação de arquivo)
- Auditoria completa de todas as tentativas
- Recovery automático em caso de falha
- Dead Letter Queue para casos irrecuperáveis
- Reconciliação periódica para detectar inconsistências
"""

import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sqlite3
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import hashlib
import json
from enum import Enum
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass, asdict
import traceback

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

# Para WSL
SOURCE_DIRECTORY = Path("/mnt/c/Automations")
DESTINATION_NETWORK_DIRECTORY = Path("/mnt/r/XML_ASINCRONIZAR/ZZZ_XML_BOT")

# Diretórios de trabalho (CRÍTICO para confiabilidade)
QUARANTINE_DIRECTORY = Path("/mnt/c/xml_organizer_data/quarantine")
PROCESSING_DIRECTORY = Path("/mnt/c/xml_organizer_data/processing")
FAILED_DIRECTORY = Path("/mnt/c/xml_organizer_data/failed")
DEAD_LETTER_DIRECTORY = Path("/mnt/c/xml_organizer_data/dead_letter")

# Banco de dados e logs
DATABASE_FILE = "/mnt/c/xml_organizer_data/xml_organizer.db"
LOG_FILE = "/mnt/c/xml_organizer_data/xml_organizer.log"
AUDIT_LOG_FILE = "/mnt/c/xml_organizer_data/audit.log"

# Parâmetros de processamento
MAX_WORKERS = 4  # Reduzido para maior controle
SCAN_INTERVAL = 30
BATCH_SIZE = 50  # Reduzido para commits mais frequentes
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY_BASE = 2  # segundos (crescimento exponencial)
RECONCILIATION_INTERVAL = 300  # 5 minutos

# Criar diretórios necessários
for directory in [
    Path(DATABASE_FILE).parent,
    QUARANTINE_DIRECTORY,
    PROCESSING_DIRECTORY,
    FAILED_DIRECTORY,
    DEAD_LETTER_DIRECTORY,
    DESTINATION_NETWORK_DIRECTORY / "_ERROS"
]:
    directory.mkdir(parents=True, exist_ok=True)

# ============================================================================
# LOGGING APRIMORADO
# ============================================================================

# Logger principal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('XMLOrganizer')

# Logger de auditoria (separado, nunca é rotacionado)
audit_logger = logging.getLogger('Audit')
audit_logger.setLevel(logging.INFO)
audit_handler = logging.FileHandler(AUDIT_LOG_FILE, encoding='utf-8')
audit_handler.setFormatter(logging.Formatter(
    '%(asctime)s [AUDIT] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
audit_logger.addHandler(audit_handler)
audit_logger.propagate = False

# ============================================================================
# ENUMS E DATACLASSES
# ============================================================================

class ProcessingStatus(Enum):
    """Status de processamento de um arquivo."""
    PENDING = "PENDING"              # Na fila para processar
    QUARANTINED = "QUARANTINED"      # Movido para quarentena
    PROCESSING = "PROCESSING"        # Em processamento
    PARSED = "PARSED"                # XML parseado com sucesso
    DB_INSERTED = "DB_INSERTED"      # Inserido no banco
    FILE_MOVED = "FILE_MOVED"        # Arquivo movido
    SUCCESS = "SUCCESS"              # Completamente processado
    FAILED_PARSING = "FAILED_PARSING"          # Falha ao parsear XML
    FAILED_DB = "FAILED_DB"                    # Falha no banco
    FAILED_MOVE = "FAILED_MOVE"                # Falha ao mover arquivo
    FAILED_PERMANENT = "FAILED_PERMANENT"      # Falha permanente (dead letter)
    DUPLICATE = "DUPLICATE"          # Arquivo duplicado

class ErrorType(Enum):
    """Tipos de erro possíveis."""
    XML_PARSE_ERROR = "XML_PARSE_ERROR"
    XML_INVALID_STRUCTURE = "XML_INVALID_STRUCTURE"
    DB_CONNECTION_ERROR = "DB_CONNECTION_ERROR"
    DB_INTEGRITY_ERROR = "DB_INTEGRITY_ERROR"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_PERMISSION_ERROR = "FILE_PERMISSION_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"

@dataclass
class ProcessingAttempt:
    """Registro de uma tentativa de processamento."""
    attempt_number: int
    timestamp: str
    status: str
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None

@dataclass
class FileProcessingRecord:
    """Registro completo de processamento de um arquivo."""
    original_path: str
    filename: str
    file_hash: str
    discovered_at: str
    current_status: str
    attempts: List[ProcessingAttempt]
    final_destination: Optional[str] = None
    chave_acesso: Optional[str] = None
    empresa_id: Optional[int] = None

# ============================================================================
# LOCKS E CACHES
# ============================================================================

db_lock = Lock()
cache_lock = Lock()
company_cache = {}
processed_hashes = set()
processed_keys = set()

# ============================================================================
# SETUP DO BANCO DE DADOS APRIMORADO
# ============================================================================

def setup_database():
    """Inicializa banco de dados com tabelas de auditoria."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Tabela de empresas (mantida)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS empresa (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cnpj TEXT NOT NULL UNIQUE,
            nome TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Tabela de notas fiscais (mantida)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS nota_fiscal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chave_acesso TEXT NOT NULL UNIQUE,
            hash_arquivo TEXT NOT NULL UNIQUE,
            empresa_id INTEGER NOT NULL,
            data_processamento TEXT NOT NULL,
            data_emissao TEXT NOT NULL,
            tipo_documento TEXT NOT NULL,
            caminho_arquivo TEXT NOT NULL,
            status TEXT DEFAULT 'PROCESSADO',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (empresa_id) REFERENCES empresa (id)
        )
        ''')
        
        # NOVA: Tabela de auditoria de processamento
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_hash TEXT NOT NULL,
            filename TEXT NOT NULL,
            original_path TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            current_status TEXT NOT NULL,
            attempt_count INTEGER DEFAULT 0,
            last_attempt_at TEXT,
            last_error_type TEXT,
            last_error_message TEXT,
            final_destination TEXT,
            chave_acesso TEXT,
            empresa_id INTEGER,
            completed_at TEXT,
            total_duration_ms INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # NOVA: Tabela de tentativas individuais
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            audit_id INTEGER NOT NULL,
            attempt_number INTEGER NOT NULL,
            status TEXT NOT NULL,
            error_type TEXT,
            error_message TEXT,
            stack_trace TEXT,
            duration_ms INTEGER,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (audit_id) REFERENCES processing_audit (id)
        )
        ''')
        
        # NOVA: Tabela de reconciliação
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reconciliation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT DEFAULT CURRENT_TIMESTAMP,
            files_checked INTEGER,
            inconsistencies_found INTEGER,
            issues_fixed INTEGER,
            details TEXT
        )
        ''')
        
        # Índices otimizados
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_chave_acesso ON nota_fiscal(chave_acesso)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash_arquivo ON nota_fiscal(hash_arquivo)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_empresa_id ON nota_fiscal(empresa_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_emissao ON nota_fiscal(data_emissao)')
        
        # Índices para auditoria
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_hash ON processing_audit(file_hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_status ON processing_audit(current_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_chave ON processing_audit(chave_acesso)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_attempts_audit ON processing_attempts(audit_id)')
        
        conn.commit()
        conn.close()
        logger.info("✓ Banco de dados inicializado com sistema de auditoria")
    except Exception as e:
        logger.critical(f"✗ Falha ao inicializar banco: {e}")
        raise

def migrate_old_database():
    """Migra dados do banco antigo se existir."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Verifica se há dados antigos para migrar
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        if 'empresa' in tables and 'processing_audit' not in tables:
            logger.info("→ Banco antigo detectado, criando tabelas de auditoria...")
            setup_database()
        
        conn.close()
    except Exception as e:
        logger.warning(f"Aviso na migração: {e}")

# ============================================================================
# FUNÇÕES DE AUDITORIA
# ============================================================================

def audit_log(event: str, details: dict):
    """Registra evento de auditoria (nunca falha)."""
    try:
        audit_logger.info(json.dumps({
            'event': event,
            'timestamp': datetime.now().isoformat(),
            **details
        }, ensure_ascii=False))
    except Exception as e:
        logger.error(f"Erro ao registrar auditoria: {e}")

def create_processing_record(file_path: Path, file_hash: str) -> int:
    """Cria registro inicial de processamento e retorna audit_id."""
    try:
        with db_lock:
            conn = sqlite3.connect(DATABASE_FILE, timeout=30)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO processing_audit 
                (file_hash, filename, original_path, discovered_at, current_status, attempt_count)
                VALUES (?, ?, ?, ?, ?, 0)
            ''', (
                file_hash,
                file_path.name,
                str(file_path),
                datetime.now().isoformat(),
                ProcessingStatus.PENDING.value
            ))
            
            audit_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            audit_log('FILE_DISCOVERED', {
                'audit_id': audit_id,
                'filename': file_path.name,
                'hash': file_hash
            })
            
            return audit_id
            
    except Exception as e:
        logger.error(f"Erro ao criar registro de processamento: {e}")
        return None

def update_processing_status(audit_id: int, status: ProcessingStatus, 
                            error_type: Optional[ErrorType] = None,
                            error_message: Optional[str] = None,
                            **kwargs):
    """Atualiza status de processamento."""
    try:
        with db_lock:
            conn = sqlite3.connect(DATABASE_FILE, timeout=30)
            cursor = conn.cursor()
            
            update_fields = {
                'current_status': status.value,
                'updated_at': datetime.now().isoformat()
            }
            
            if error_type:
                update_fields['last_error_type'] = error_type.value
            if error_message:
                update_fields['last_error_message'] = error_message[:500]  # Limita tamanho
            
            update_fields.update(kwargs)
            
            set_clause = ', '.join([f"{k} = ?" for k in update_fields.keys()])
            values = list(update_fields.values()) + [audit_id]
            
            cursor.execute(f'''
                UPDATE processing_audit 
                SET {set_clause}
                WHERE id = ?
            ''', values)
            
            conn.commit()
            conn.close()
            
    except Exception as e:
        logger.error(f"Erro ao atualizar status: {e}")

def record_attempt(audit_id: int, attempt_number: int, status: ProcessingStatus,
                  error_type: Optional[ErrorType] = None,
                  error_message: Optional[str] = None,
                  stack_trace: Optional[str] = None,
                  duration_ms: Optional[int] = None):
    """Registra tentativa individual de processamento."""
    try:
        with db_lock:
            conn = sqlite3.connect(DATABASE_FILE, timeout=30)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO processing_attempts
                (audit_id, attempt_number, status, error_type, error_message, stack_trace, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                audit_id,
                attempt_number,
                status.value,
                error_type.value if error_type else None,
                error_message[:500] if error_message else None,
                stack_trace[:2000] if stack_trace else None,
                duration_ms
            ))
            
            # Atualiza contador de tentativas
            cursor.execute('''
                UPDATE processing_audit
                SET attempt_count = attempt_count + 1,
                    last_attempt_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (audit_id,))
            
            conn.commit()
            conn.close()
            
            audit_log('PROCESSING_ATTEMPT', {
                'audit_id': audit_id,
                'attempt': attempt_number,
                'status': status.value,
                'error': error_type.value if error_type else None
            })
            
    except Exception as e:
        logger.error(f"Erro ao registrar tentativa: {e}")

# ============================================================================
# FUNÇÕES AUXILIARES (mantidas do original)
# ============================================================================

def calculate_file_hash(file_path: Path) -> Optional[str]:
    """Calcula hash MD5 do arquivo."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"Erro ao calcular hash de {file_path.name}: {e}")
        return None

def standardize_company_name(name: str) -> str:
    """Padroniza nome da empresa."""
    name = re.sub(r'[.\-/\\]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.upper()

def get_or_create_company(cnpj: str, nome_xml: str) -> int:
    """Obtém ou cria empresa no banco."""
    nome_padronizado = standardize_company_name(nome_xml)
    
    with cache_lock:
        if cnpj in company_cache:
            cached = company_cache[cnpj]
            if cached["nome"] != nome_padronizado:
                with db_lock:
                    conn = sqlite3.connect(DATABASE_FILE, timeout=10)
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE empresa SET nome = ?, updated_at = CURRENT_TIMESTAMP WHERE cnpj = ?",
                        (nome_padronizado, cnpj)
                    )
                    conn.commit()
                    conn.close()
                company_cache[cnpj]["nome"] = nome_padronizado
            return cached["id"]
        
        with db_lock:
            conn = sqlite3.connect(DATABASE_FILE, timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT id, nome FROM empresa WHERE cnpj = ?", (cnpj,))
            result = cursor.fetchone()
            
            if result:
                company_id, nome_atual = result
                if nome_atual != nome_padronizado:
                    cursor.execute(
                        "UPDATE empresa SET nome = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (nome_padronizado, company_id)
                    )
                    conn.commit()
            else:
                cursor.execute(
                    "INSERT INTO empresa (cnpj, nome) VALUES (?, ?)",
                    (cnpj, nome_padronizado)
                )
                conn.commit()
                company_id = cursor.lastrowid
            
            conn.close()
            company_cache[cnpj] = {"id": company_id, "nome": nome_padronizado}
            return company_id

def get_xml_info(xml_file: Path) -> Optional[dict]:
    """Extrai informações do XML (mantida lógica original)."""
    namespaces = [
        {'nfe': 'http://www.portalfiscal.inf.br/nfe'},
        {},
    ]
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        infNFe = None
        for ns in namespaces:
            infNFe = root.find('.//nfe:infNFe', ns) if ns else root.find('.//infNFe')
            if infNFe is not None:
                break
        
        if infNFe is None:
            for elem in root.iter():
                if elem.tag.endswith('infNFe'):
                    infNFe = elem
                    break
        
        if infNFe is None:
            return None

        chave_acesso = infNFe.get('Id', '').replace('NFe', '').replace('nfe', '')

        ide = None
        emit = None
        for ns in namespaces:
            if ns:
                ide = infNFe.find('nfe:ide', ns)
                emit = infNFe.find('nfe:emit', ns)
            else:
                ide = infNFe.find('ide')
                emit = infNFe.find('emit')
            if ide is not None and emit is not None:
                break

        if ide is None or emit is None:
            return None

        data_emissao_str = None
        for tag_name in ['dhEmi', 'dEmi']:
            for ns in namespaces:
                elem = ide.find(f'nfe:{tag_name}', ns) if ns else ide.find(tag_name)
                if elem is not None:
                    data_emissao_str = elem.text.split('T')[0] if 'T' in elem.text else elem.text
                    break
            if data_emissao_str:
                break
        
        if not data_emissao_str:
            return None
            
        data_emissao_dt = datetime.strptime(data_emissao_str, '%Y-%m-%d')

        modelo = None
        for ns in namespaces:
            mod_elem = ide.find('nfe:mod', ns) if ns else ide.find('mod')
            if mod_elem is not None:
                modelo = mod_elem.text
                break
        
        tipo_documento = 'NFE' if modelo == '55' else 'NFCE' if modelo == '65' else f"MOD{modelo}"

        cnpj = None
        nome_empresa = None
        for ns in namespaces:
            cnpj_elem = emit.find('nfe:CNPJ', ns) if ns else emit.find('CNPJ')
            nome_elem = emit.find('nfe:xNome', ns) if ns else emit.find('xNome')
            if cnpj_elem is not None:
                cnpj = cnpj_elem.text
            if nome_elem is not None:
                nome_empresa = nome_elem.text
            if cnpj and nome_empresa:
                break

        if not cnpj or not nome_empresa:
            return None

        return {
            "data_processamento": datetime.now().strftime('%Y-%m-%d'),
            "data_emissao": data_emissao_dt.strftime('%Y-%m-%d'),
            "chave_acesso": chave_acesso,
            "empresa_nome_xml": nome_empresa,
            "empresa_nome_padronizado": standardize_company_name(nome_empresa),
            "cnpj": cnpj,
            "tipo_documento": tipo_documento,
            "ano_emissao": data_emissao_dt.strftime('%Y'),
            "mes_ano_emissao": data_emissao_dt.strftime('%m-%Y'),
            "dia_emissao": data_emissao_dt.strftime('%d')
        }

    except Exception as e:
        logger.error(f"Erro ao parsear XML {xml_file.name}: {e}")
        return None

def load_caches():
    """Carrega caches de empresas e arquivos processados."""
    global company_cache, processed_hashes, processed_keys
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT cnpj, id, nome FROM empresa")
        for cnpj, empresa_id, nome in cursor.fetchall():
            company_cache[cnpj] = {"id": empresa_id, "nome": nome}
        
        cursor.execute("SELECT hash_arquivo, chave_acesso FROM nota_fiscal")
        for hash_arq, chave in cursor.fetchall():
            processed_hashes.add(hash_arq)
            processed_keys.add(chave)
        
        conn.close()
        logger.info(f"✓ Cache: {len(company_cache)} empresas, {len(processed_hashes)} registros")
    except Exception as e:
        logger.error(f"Erro ao carregar cache: {e}")

# ============================================================================
# PROCESSAMENTO COM RETRY E TRANSAÇÃO ATÔMICA
# ============================================================================

def move_to_quarantine(source: Path) -> Optional[Path]:
    """
    Move arquivo para quarentena ANTES de processar.
    CRÍTICO: Garante que arquivo não seja perdido se algo der errado.
    """
    try:
        # Nome único com timestamp para evitar conflitos
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        quarantine_file = QUARANTINE_DIRECTORY / f"{timestamp}_{source.name}"
        
        shutil.move(str(source), str(quarantine_file))
        audit_log('FILE_QUARANTINED', {
            'original': str(source),
            'quarantine': str(quarantine_file)
        })
        return quarantine_file
        
    except Exception as e:
        logger.error(f"CRÍTICO: Falha ao mover para quarentena {source.name}: {e}")
        audit_log('QUARANTINE_FAILED', {
            'file': str(source),
            'error': str(e)
        })
        return None

def atomic_process_file(quarantine_file: Path, audit_id: int, attempt_number: int) -> Tuple[bool, Optional[str], Optional[ErrorType]]:
    """
    Processa arquivo de forma atômica com rollback em caso de falha.
    
    Returns:
        (success, error_message, error_type)
    """
    start_time = time.time()
    conn = None
    empresa_id = None
    chave_acesso = None
    destination_path = None
    
    try:
        # FASE 1: Calcular hash
        file_hash = calculate_file_hash(quarantine_file)
        if not file_hash:
            return False, "Falha ao calcular hash", ErrorType.FILE_NOT_FOUND
        
        # Verificar duplicata por hash
        if file_hash in processed_hashes:
            update_processing_status(audit_id, ProcessingStatus.DUPLICATE)
            quarantine_file.unlink()
            return True, "Duplicado (hash)", None  # Sucesso (arquivo já processado)
        
        # FASE 2: Parsear XML
        info = get_xml_info(quarantine_file)
        if not info:
            return False, "XML inválido ou incompleto", ErrorType.XML_INVALID_STRUCTURE
        
        chave_acesso = info["chave_acesso"]
        
        # Verificar duplicata por chave
        if chave_acesso in processed_keys:
            update_processing_status(audit_id, ProcessingStatus.DUPLICATE, chave_acesso=chave_acesso)
            quarantine_file.unlink()
            return True, "Duplicado (chave)", None
        
        update_processing_status(audit_id, ProcessingStatus.PARSED, chave_acesso=chave_acesso)
        
        # FASE 3: Obter/criar empresa
        empresa_id = get_or_create_company(info["cnpj"], info["empresa_nome_xml"])
        nome_empresa_final = company_cache[info["cnpj"]]["nome"]
        info["empresa_nome_padronizado"] = nome_empresa_final
        
        # FASE 4: Preparar destino
        destination_path = (
            DESTINATION_NETWORK_DIRECTORY /
            f"{nome_empresa_final} - {info['cnpj']}" /
            info['tipo_documento'] /
            info['ano_emissao'] /
            info['mes_ano_emissao'] /
            info['dia_emissao'] /
            quarantine_file.name
        )
        
        # Verificar se destino já existe (duplicata)
        if destination_path.exists():
            update_processing_status(audit_id, ProcessingStatus.DUPLICATE, 
                                    chave_acesso=chave_acesso, 
                                    empresa_id=empresa_id)
            quarantine_file.unlink()
            return True, "Duplicado (destino existe)", None
        
        # FASE 5: TRANSAÇÃO ATÔMICA - BD + Movimentação
        # Tenta inserir no BD primeiro
        conn = sqlite3.connect(DATABASE_FILE, timeout=30)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO nota_fiscal 
                (chave_acesso, hash_arquivo, empresa_id, data_processamento, 
                 data_emissao, tipo_documento, caminho_arquivo)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                chave_acesso,
                file_hash,
                empresa_id,
                info["data_processamento"],
                info["data_emissao"],
                info["tipo_documento"],
                str(destination_path)
            ))
            
            # Commit no BD
            conn.commit()
            update_processing_status(audit_id, ProcessingStatus.DB_INSERTED,
                                    chave_acesso=chave_acesso,
                                    empresa_id=empresa_id)
            
        except sqlite3.IntegrityError as e:
            conn.rollback()
            update_processing_status(audit_id, ProcessingStatus.DUPLICATE)
            quarantine_file.unlink()
            return True, f"Duplicado (BD): {e}", None
        
        except Exception as e:
            conn.rollback()
            return False, f"Erro BD: {e}", ErrorType.DB_CONNECTION_ERROR
        
        finally:
            if conn:
                conn.close()
        
        # FASE 6: Mover arquivo físico
        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(quarantine_file), str(destination_path))
            
            update_processing_status(
                audit_id, 
                ProcessingStatus.FILE_MOVED,
                final_destination=str(destination_path)
            )
            
        except Exception as e:
            # ROLLBACK: Remover do BD se falhou ao mover arquivo
            logger.error(f"CRÍTICO: Falha ao mover arquivo, fazendo rollback do BD: {e}")
            
            try:
                conn = sqlite3.connect(DATABASE_FILE, timeout=30)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM nota_fiscal WHERE chave_acesso = ?", (chave_acesso,))
                conn.commit()
                conn.close()
                logger.info(f"✓ Rollback do BD executado com sucesso")
            except Exception as rollback_error:
                logger.critical(f"FALHA NO ROLLBACK: {rollback_error}")
            
            return False, f"Erro ao mover: {e}", ErrorType.FILE_PERMISSION_ERROR
        
        # FASE 7: Atualizar caches
        processed_hashes.add(file_hash)
        processed_keys.add(chave_acesso)
        
        # FASE 8: Marcar como sucesso
        duration_ms = int((time.time() - start_time) * 1000)
        update_processing_status(
            audit_id,
            ProcessingStatus.SUCCESS,
            final_destination=str(destination_path),
            completed_at=datetime.now().isoformat(),
            total_duration_ms=duration_ms
        )
        
        audit_log('FILE_PROCESSED_SUCCESS', {
            'audit_id': audit_id,
            'chave': chave_acesso,
            'empresa_id': empresa_id,
            'destination': str(destination_path),
            'duration_ms': duration_ms
        })
        
        return True, None, None
        
    except Exception as e:
        error_msg = f"Erro inesperado: {str(e)}"
        stack = traceback.format_exc()
        logger.error(f"{error_msg}\n{stack}")
        return False, error_msg, ErrorType.UNKNOWN_ERROR

def process_single_file_with_retry(source_file: Path) -> dict:
    """
    Processa um arquivo com retry automático e auditoria completa.
    
    Fluxo:
    1. Calcula hash inicial
    2. Cria registro de auditoria
    3. Move para quarentena (staging)
    4. Tenta processar com retry exponencial
    5. Se falhar permanentemente, move para dead letter
    """
    result = {
        "file": source_file.name,
        "status": "erro",
        "audit_id": None,
        "attempts": 0
    }
    
    try:
        # 1. Hash inicial (enquanto ainda está no source)
        file_hash = calculate_file_hash(source_file)
        if not file_hash:
            logger.error(f"Impossível calcular hash: {source_file.name}")
            return result
        
        # 2. Criar registro de auditoria
        audit_id = create_processing_record(source_file, file_hash)
        if not audit_id:
            logger.error(f"Impossível criar registro de auditoria: {source_file.name}")
            return result
        
        result["audit_id"] = audit_id
        
        # 3. Mover para quarentena (CRÍTICO)
        quarantine_file = move_to_quarantine(source_file)
        if not quarantine_file:
            update_processing_status(audit_id, ProcessingStatus.FAILED_PERMANENT,
                                    error_type=ErrorType.FILE_PERMISSION_ERROR,
                                    error_message="Falha ao mover para quarentena")
            return result
        
        update_processing_status(audit_id, ProcessingStatus.QUARANTINED)
        
        # 4. Tentar processar com retry
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            result["attempts"] = attempt
            
            logger.info(f"→ Processando {quarantine_file.name} (tentativa {attempt}/{MAX_RETRY_ATTEMPTS})")
            
            update_processing_status(audit_id, ProcessingStatus.PROCESSING)
            
            success, error_msg, error_type = atomic_process_file(quarantine_file, audit_id, attempt)
            
            # Registrar tentativa
            status = ProcessingStatus.SUCCESS if success else ProcessingStatus.FAILED_MOVE
            record_attempt(audit_id, attempt, status, error_type, error_msg,
                         traceback.format_exc() if not success else None)
            
            if success:
                result["status"] = "sucesso"
                logger.info(f"✓ {quarantine_file.name} processado com sucesso")
                return result
            
            # Se falhou, verificar se deve tentar novamente
            if attempt < MAX_RETRY_ATTEMPTS:
                delay = RETRY_DELAY_BASE ** attempt  # Backoff exponencial
                logger.warning(f"⚠ Tentativa {attempt} falhou: {error_msg}. Tentando novamente em {delay}s...")
                time.sleep(delay)
            else:
                # Esgotou tentativas
                logger.error(f"✗ FALHA PERMANENTE após {MAX_RETRY_ATTEMPTS} tentativas: {quarantine_file.name}")
                
                # Mover para dead letter queue
                dead_letter_file = DEAD_LETTER_DIRECTORY / quarantine_file.name
                try:
                    shutil.move(str(quarantine_file), str(dead_letter_file))
                    update_processing_status(audit_id, ProcessingStatus.FAILED_PERMANENT,
                                            error_type=error_type,
                                            error_message=error_msg,
                                            final_destination=str(dead_letter_file))
                    
                    audit_log('FILE_DEAD_LETTER', {
                        'audit_id': audit_id,
                        'file': quarantine_file.name,
                        'attempts': MAX_RETRY_ATTEMPTS,
                        'final_error': error_msg
                    })
                except Exception as e:
                    logger.critical(f"CRÍTICO: Falha ao mover para dead letter: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Erro crítico no processamento de {source_file.name}: {e}")
        logger.error(traceback.format_exc())
        return result

# ============================================================================
# PROCESSAMENTO EM LOTE
# ============================================================================

def process_batch(xml_files: List[Path]) -> dict:
    """Processa lote de arquivos com controle de erro."""
    stats = {
        "sucesso": 0,
        "duplicado": 0,
        "erro": 0,
        "total_attempts": 0
    }
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_file_with_retry, f): f for f in xml_files}
        
        for future in as_completed(futures):
            try:
                result = future.result(timeout=60)  # Timeout maior devido aos retries
                
                stats["total_attempts"] += result.get("attempts", 0)
                
                if result["status"] == "sucesso":
                    stats["sucesso"] += 1
                elif "duplicado" in result.get("status", ""):
                    stats["duplicado"] += 1
                else:
                    stats["erro"] += 1
                    
            except Exception as e:
                stats["erro"] += 1
                logger.error(f"Erro no future: {e}")
    
    return stats

def scan_and_process():
    """Escaneia diretório de origem e processa arquivos."""
    if not SOURCE_DIRECTORY.exists():
        logger.error(f"Diretório de origem não encontrado: {SOURCE_DIRECTORY}")
        return
    
    xml_files = list(SOURCE_DIRECTORY.rglob("*.xml"))
    
    if not xml_files:
        return
    
    total = len(xml_files)
    logger.info(f"→ {total} arquivo(s) encontrado(s)")
    
    start_time = time.time()
    total_stats = {"sucesso": 0, "duplicado": 0, "erro": 0, "total_attempts": 0}
    batch_num = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch = xml_files[i:i+BATCH_SIZE]
        batch_num += 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        
        stats = process_batch(batch)
        
        for key in total_stats:
            total_stats[key] += stats[key]
        
        processed = total_stats["sucesso"] + total_stats["duplicado"] + total_stats["erro"]
        elapsed = time.time() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"✓ Lote {batch_num}/{total_batches}: "
            f"{stats['sucesso']} ok | {stats['duplicado']} dup | {stats['erro']} erro | "
            f"{stats['total_attempts']} tentativas | {processed}/{total} ({rate:.1f} arq/s)"
        )
    
    elapsed = time.time() - start_time
    if processed > 0:
        logger.info(
            f"✓ CONCLUÍDO: {total_stats['sucesso']} novos | "
            f"{total_stats['duplicado']} duplicados | {total_stats['erro']} erros | "
            f"{total_stats['total_attempts']} tentativas | "
            f"Tempo: {elapsed:.1f}s | Taxa: {total/elapsed:.1f} arq/s"
        )

# ============================================================================
# RECONCILIAÇÃO E RECOVERY
# ============================================================================

def reconcile_processing_queue():
    """
    Verifica arquivos que ficaram presos em estados intermediários e tenta recuperá-los.
    Executa periodicamente para garantir que nenhum arquivo seja esquecido.
    """
    try:
        logger.info("🔍 Iniciando reconciliação...")
        
        issues_found = []
        issues_fixed = 0
        
        # 1. Verificar arquivos em quarentena que não têm registro de processamento recente
        quarantine_files = list(QUARANTINE_DIRECTORY.glob("*.xml"))
        if quarantine_files:
            logger.warning(f"⚠ {len(quarantine_files)} arquivo(s) em quarentena precisam atenção")
            
            for qfile in quarantine_files:
                # Verificar quanto tempo está em quarentena
                file_age_seconds = time.time() - qfile.stat().st_mtime
                
                if file_age_seconds > 300:  # Mais de 5 minutos
                    logger.warning(f"⚠ Arquivo preso em quarentena: {qfile.name} ({file_age_seconds:.0f}s)")
                    issues_found.append(f"Quarantine stuck: {qfile.name}")
                    
                    # Tentar processar novamente
                    try:
                        result = process_single_file_with_retry(qfile)
                        if result["status"] == "sucesso":
                            issues_fixed += 1
                    except Exception as e:
                        logger.error(f"Erro ao recuperar {qfile.name}: {e}")
        
        # 2. Verificar registros no BD com status intermediário
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        intermediate_statuses = [
            ProcessingStatus.PENDING.value,
            ProcessingStatus.QUARANTINED.value,
            ProcessingStatus.PROCESSING.value,
            ProcessingStatus.PARSED.value,
            ProcessingStatus.DB_INSERTED.value
        ]
        
        cursor.execute('''
            SELECT id, filename, current_status, last_attempt_at, attempt_count
            FROM processing_audit
            WHERE current_status IN ({})
            AND datetime(last_attempt_at) < datetime('now', '-10 minutes')
        '''.format(','.join(['?'] * len(intermediate_statuses))), intermediate_statuses)
        
        stuck_records = cursor.fetchall()
        
        if stuck_records:
            logger.warning(f"⚠ {len(stuck_records)} registro(s) com status intermediário antigo")
            
            for record_id, filename, status, last_attempt, attempt_count in stuck_records:
                logger.warning(f"⚠ Registro preso: ID={record_id}, {filename}, status={status}")
                issues_found.append(f"DB stuck: {filename} ({status})")
                
                # Verificar se arquivo ainda existe em algum lugar
                # Se não existe, marcar como perdido
                found = False
                for directory in [QUARANTINE_DIRECTORY, PROCESSING_DIRECTORY, FAILED_DIRECTORY]:
                    if (directory / filename).exists():
                        found = True
                        break
                
                if not found:
                    logger.error(f"✗ Arquivo perdido: {filename} (ID={record_id})")
                    cursor.execute('''
                        UPDATE processing_audit
                        SET current_status = ?, 
                            last_error_message = 'Arquivo perdido durante reconciliação',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (ProcessingStatus.FAILED_PERMANENT.value, record_id))
        
        # 3. Verificar dead letter queue
        dead_letter_files = list(DEAD_LETTER_DIRECTORY.glob("*.xml"))
        if dead_letter_files:
            logger.warning(f"⚠ {len(dead_letter_files)} arquivo(s) em dead letter queue")
            issues_found.append(f"Dead letter: {len(dead_letter_files)} files")
        
        conn.commit()
        
        # 4. Registrar resultado da reconciliação
        cursor.execute('''
            INSERT INTO reconciliation_log
            (files_checked, inconsistencies_found, issues_fixed, details)
            VALUES (?, ?, ?, ?)
        ''', (
            len(quarantine_files) + len(stuck_records),
            len(issues_found),
            issues_fixed,
            json.dumps(issues_found, ensure_ascii=False)
        ))
        
        conn.commit()
        conn.close()
        
        if issues_found:
            logger.warning(f"⚠ Reconciliação: {len(issues_found)} problema(s), {issues_fixed} corrigido(s)")
        else:
            logger.info("✓ Reconciliação: Nenhum problema encontrado")
        
        audit_log('RECONCILIATION_COMPLETED', {
            'issues_found': len(issues_found),
            'issues_fixed': issues_fixed,
            'details': issues_found
        })
        
    except Exception as e:
        logger.error(f"Erro na reconciliação: {e}")
        logger.error(traceback.format_exc())

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Loop principal do sistema."""
    logger.info("=" * 80)
    logger.info("XML ORGANIZER v3.0 - ULTRA-CONFIÁVEL")
    logger.info(f"Monitorando: {SOURCE_DIRECTORY}")
    logger.info(f"Destino: {DESTINATION_NETWORK_DIRECTORY}")
    logger.info(f"Quarentena: {QUARANTINE_DIRECTORY}")
    logger.info(f"Dead Letter: {DEAD_LETTER_DIRECTORY}")
    logger.info(f"Banco de dados: {DATABASE_FILE}")
    logger.info(f"Workers: {MAX_WORKERS} | Batch: {BATCH_SIZE} | Max Retry: {MAX_RETRY_ATTEMPTS}")
    logger.info("=" * 80)
    
    setup_database()
    migrate_old_database()
    load_caches()
    
    logger.info("\n🛡️ CAMADAS DE PROTEÇÃO ATIVAS:")
    logger.info("  1. ✓ Quarentena antes de processar")
    logger.info("  2. ✓ Retry automático com backoff exponencial")
    logger.info("  3. ✓ Transação atômica (BD + arquivo)")
    logger.info("  4. ✓ Auditoria completa de tentativas")
    logger.info("  5. ✓ Dead Letter Queue para falhas permanentes")
    logger.info("  6. ✓ Reconciliação periódica")
    logger.info("  7. ✓ Recovery automático\n")
    
    audit_log('SYSTEM_STARTED', {'version': '3.0'})
    
    cycle = 0
    last_reconciliation = time.time()
    
    while True:
        try:
            cycle += 1
            
            # Processar arquivos
            scan_and_process()
            
            # Reconciliação periódica
            if time.time() - last_reconciliation > RECONCILIATION_INTERVAL:
                reconcile_processing_queue()
                last_reconciliation = time.time()
            
            time.sleep(SCAN_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("\n⊗ Finalizando por solicitação do usuário")
            audit_log('SYSTEM_STOPPED', {'reason': 'user_request', 'cycles': cycle})
            break
            
        except Exception as e:
            logger.error(f"✗ Erro no ciclo {cycle}: {e}")
            logger.error(traceback.format_exc())
            audit_log('SYSTEM_ERROR', {'cycle': cycle, 'error': str(e)})
            time.sleep(10)

if __name__ == "__main__":
    main()
