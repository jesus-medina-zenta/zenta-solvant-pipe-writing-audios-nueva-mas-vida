import asyncio
import threading
from typing import List, Dict, Any
import paramiko
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SFTPService:
    """
    Servicio SFTP thread-safe para subida de archivos.
    """
    
    def __init__(self, sftp_config):
        self.config = sftp_config
        self._lock = asyncio.Lock()  # Lock para operaciones SFTP
        
    async def load(self, upload_data: List[Dict[str, Any]]) -> bool:
        """
        Sube archivos al servidor SFTP de forma thread-safe.
        """
        async with self._lock:  # Asegurar que solo una operación SFTP a la vez
            return await self._upload_files_sync(upload_data)
    
    async def _upload_files_sync(self, upload_data: List[Dict[str, Any]]) -> bool:
        """
        Método interno para subir archivos de forma síncrona.
        """
        ssh_client = None
        sftp_client = None
        
        try:
            # Crear nueva conexión para cada operación
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Conectar con timeout
            logger.debug(f"🔗 Conectando a SFTP: {self.config.host}:{self.config.port}")
            ssh_client.connect(
                hostname=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                timeout=30,
                auth_timeout=30
            )
            
            sftp_client = ssh_client.open_sftp()
            logger.debug("✅ Conexión SFTP establecida")
            
            # Subir archivos
            logger.info(f"📤 Iniciando subida de {len(upload_data)} archivos...")
            
            successful_uploads = 0
            for i, file_info in enumerate(upload_data):
                try:
                    local_path = file_info["local_path"]
                    remote_path = file_info["remote_path"]
                    filename = file_info["target_filename"]
                    
                    logger.info(f"📤 [{i+1}/{len(upload_data)}] Subiendo: {filename}")
                    
                    # Crear directorio remoto si no existe (incluyendo subcarpetas nuevas, ej. por fecha)
                    remote_dir = '/'.join(remote_path.split('/')[:-1])
                    try:
                        self._ensure_remote_dir(sftp_client, remote_dir)
                    except Exception as mkdir_error:
                        logger.warning(f"⚠️ No se pudo verificar/crear directorio remoto {remote_dir}: {mkdir_error}")
                    
                    # Subir archivo
                    sftp_client.put(local_path, remote_path)
                    logger.info(f"✅ [{i+1}/{len(upload_data)}] Subido: {filename} → {remote_path}")
                    successful_uploads += 1
                    
                except Exception as upload_error:
                    logger.error(f"❌ Error subiendo {filename}: {upload_error}")
                    continue
            
            logger.info(f"📊 Subida completada: {successful_uploads}/{len(upload_data)} archivos exitosos")
            return successful_uploads == len(upload_data)
            
        except Exception as e:
            logger.error(f"❌ Error en conexión SFTP: {e}")
            return False
            
        finally:
            # Cerrar conexiones
            try:
                if sftp_client:
                    sftp_client.close()
                if ssh_client:
                    ssh_client.close()
                logger.debug("🔌 Conexión SFTP cerrada")
            except Exception as e:
                logger.warning(f"⚠️ Error cerrando conexión SFTP: {e}")

    def _ensure_remote_dir(self, sftp_client, remote_dir: str) -> None:
        """
        Crea recursivamente un directorio remoto si no existe.
        paramiko.SFTPClient no trae 'makedirs', solo 'mkdir' de un nivel.
        """
        if not remote_dir or remote_dir in (".", "/"):
            return

        current = ""
        for part in remote_dir.split("/"):
            if not part:
                continue
            current += f"/{part}"
            try:
                sftp_client.stat(current)
            except FileNotFoundError:
                sftp_client.mkdir(current)

    async def disconnect(self) -> None:
        """Método para compatibilidad con el pipeline."""
        # No necesario ya que cada operación maneja su propia conexión
        pass