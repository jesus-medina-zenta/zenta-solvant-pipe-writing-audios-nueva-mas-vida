import paramiko
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional

from ..utils.logger import get_logger
from .base_service import BaseService

logger = get_logger(__name__)

class SFTPService(BaseService):
    """
    Servicio para operaciones SFTP usando Paramiko.
    """
    def __init__(self, connection_config: Optional[Dict[str, Any]] = None):
        super().__init__(connection_config)
        self.transport = None
        self.sftp_client = None

    async def connect(self) -> bool:
        """Establece la conexión SFTP de forma asíncrona"""
        try:
            loop = asyncio.get_event_loop()
            
            def _connect():
                self.transport = paramiko.Transport(
                    (self.connection_config.host, self.connection_config.port)
                )
                self.transport.connect(
                    username=self.connection_config.username,
                    password=self.connection_config.password
                )
                self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)
                return True
            
            await loop.run_in_executor(None, _connect)
            self.is_connected = True
            logger.info("✅ Conexión SFTP exitosa")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando a SFTP: {e}")
            self.is_connected = False
            return False

    async def disconnect(self) -> None:
        """Cierra la conexión SFTP de forma asíncrona"""
        try:
            loop = asyncio.get_event_loop()
            
            def _disconnect():
                if self.sftp_client:
                    self.sftp_client.close()
                if self.transport:
                    self.transport.close()
            
            await loop.run_in_executor(None, _disconnect)
            self.is_connected = False
            logger.info("🔌 Desconexión SFTP exitosa")
            
        except Exception as e:
            logger.error(f"Error cerrando conexión SFTP: {e}")

    def extract(self, *args, **kwargs):
        """Método stub para cumplir con la interfaz abstracta."""
        logger.warning("Extracción no soportada en SFTPService")
        return None

    async def load(self, data: List[Dict[str, Any]], **kwargs) -> bool:
        """
        Sube archivos al servidor SFTP usando rutas fijas existentes.
        
        Args:
            data: Lista de archivos a subir
                Formato: [{"local_path": "path", "remote_path": "path"}]
            
        Returns:
            bool: True si la carga fue exitosa
        """
        if not data:
            logger.warning("⚠️ No hay archivos para subir a SFTP")
            return True
        
        if not self.is_connected:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            success_count = 0
            total_files = len(data)
            
            logger.info(f"📤 Iniciando subida de {total_files} archivos...")
            
            for i, file_info in enumerate(data, 1):
                local_path = file_info.get("local_path")
                remote_path = file_info.get("remote_path")
                
                if not local_path or not remote_path:
                    logger.warning(f"⚠️ Datos incompletos: {file_info}")
                    continue
                
                if not Path(local_path).exists():
                    logger.error(f"❌ Archivo local no encontrado: {local_path}")
                    continue
                
                try:
                    def _upload_file():
                        # NO CREAR DIRECTORIOS - asumir que la ruta base ya existe
                        # Simplemente subir el archivo a la ruta especificada
                        self.sftp_client.put(local_path, remote_path)
                        return True
                    
                    logger.info(f"📤 [{i}/{total_files}] Subiendo: {Path(local_path).name}")
                    
                    # Ejecutar subida con timeout
                    await asyncio.wait_for(
                        loop.run_in_executor(None, _upload_file),
                        timeout=60.0  # 60 segundos timeout por archivo
                    )
                    
                    success_count += 1
                    logger.info(f"✅ [{i}/{total_files}] Subido: {Path(local_path).name} → {remote_path}")
                    
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Timeout subiendo archivo: {Path(local_path).name}")
                except Exception as e:
                    logger.error(f"❌ Error subiendo {Path(local_path).name}: {e}")
            
            logger.info(f"📊 Subida completada: {success_count}/{total_files} archivos exitosos")
            return success_count == total_files
            
        except Exception as e:
            logger.error(f"❌ Error general subiendo archivos a SFTP: {e}")
            return False