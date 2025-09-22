import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden

from .base_service import BaseService
from ..utils.logger import get_logger

logger = get_logger(__name__)


class CloudStorageService(BaseService):
    """
    Servicio para manejar conexiones y operaciones con Google Cloud Storage.
    """
    
    def __init__(self, project_id: Optional[str] = None, bucket_name: Optional[str] = None):
        """
        Inicializa el servicio de Cloud Storage.
        
        Args:
            project_id: ID del proyecto de Google Cloud
            bucket_name: Nombre del bucket
        """
        super().__init__()
        self.client: Optional[storage.Client] = None
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.bucket = None
        
    async def connect(self) -> bool:
        """
        Establece conexión con Cloud Storage.
        
        Returns:
            bool: True si la conexión fue exitosa
        """
        try:
            logger.info("Conectando a Cloud Storage...")
            
            # Crear cliente de Cloud Storage
            if self.project_id:
                self.client = storage.Client(project=self.project_id)
            else:
                self.client = storage.Client()
            
            # Obtener referencia al bucket sin verificar permisos
            if self.bucket_name:
                self.bucket = self.client.bucket(self.bucket_name)
                # NO llamamos bucket.exists() para evitar error de permisos
                logger.info(f"📂 Bucket configurado: {self.bucket_name}")
            
            self.is_connected = True
            logger.info("✅ Conexión a Cloud Storage establecida exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando a Cloud Storage: {e}")
            self.is_connected = False
            return False
    
    async def disconnect(self) -> None:
        """Cierra la conexión a Cloud Storage."""
        if self.client:
            self.client.close()
        self.is_connected = False
        logger.info("🔌 Desconectado de Cloud Storage")
    
    async def extract(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Extrae metadatos de archivos desde Cloud Storage.
        
        Args:
            filters: Filtros opcionales (prefix, limit, extensions)
            
        Returns:
            List[Dict[str, Any]]: Lista de metadatos de archivos
        """
        if not self.is_connected:
            await self.connect()
        
        if not self.bucket:
            raise RuntimeError("No hay bucket configurado")
        
        try:
            prefix = filters.get("prefix") if filters else None
            limit = filters.get("limit") if filters else None
            extensions = filters.get("extensions", []) if filters else []
            
            loop = asyncio.get_event_loop()
            
            def _list_blobs():
                blobs = list(self.bucket.list_blobs(prefix=prefix, max_results=limit))
                return blobs
            
            blobs = await loop.run_in_executor(None, _list_blobs)
            
            result = []
            for blob in blobs:
                # Filtrar por extensiones si se especifican
                if extensions and not any(blob.name.lower().endswith(ext) for ext in extensions):
                    continue
                    
                result.append({
                    "name": blob.name,
                    "size": blob.size,
                    "content_type": blob.content_type,
                    "etag": blob.etag,
                    "time_created": blob.time_created.isoformat() if blob.time_created else None,
                    "updated": blob.updated.isoformat() if blob.updated else None,
                    "md5_hash": blob.md5_hash,
                    "bucket": self.bucket_name
                })
            
            logger.info(f"📊 Extraídos metadatos de {len(result)} archivos de Cloud Storage")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos de Cloud Storage: {e}")
            return []
    
    async def load(self, data: List[Dict[str, Any]]) -> bool:
        """
        Carga archivos a Cloud Storage.
        
        Args:
            data: Lista de diccionarios con información de archivos
                  Formato: [{"local_path": "path", "blob_name": "name", "content_type": "type"}]
            
        Returns:
            bool: True si la carga fue exitosa
        """
        if not data:
            logger.warning("⚠️ No hay datos para cargar en Cloud Storage")
            return True
        
        if not self.is_connected:
            await self.connect()
        
        if not self.bucket:
            raise RuntimeError("No hay bucket configurado")
        
        try:
            success_count = 0
            loop = asyncio.get_event_loop()
            
            for item in data:
                local_path = item.get("local_path")
                blob_name = item.get("blob_name")
                content_type = item.get("content_type", "application/octet-stream")
                
                if not local_path or not blob_name:
                    logger.warning(f"⚠️ Datos incompletos: {item}")
                    continue
                
                if not Path(local_path).exists():
                    logger.error(f"❌ Archivo local no encontrado: {local_path}")
                    continue
                
                def _upload_blob():
                    blob = self.bucket.blob(blob_name)
                    blob.upload_from_filename(local_path, content_type=content_type)
                    return True
                
                await loop.run_in_executor(None, _upload_blob)
                success_count += 1
                logger.info(f"✅ Archivo subido: {local_path} → {blob_name}")
            
            logger.info(f"📤 Cargados {success_count}/{len(data)} archivos en Cloud Storage")
            return success_count == len(data)
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos en Cloud Storage: {e}")
            return False
    
    async def download_file(self, blob_name: str, local_path: str) -> bool:
        """
        Descarga un archivo desde Cloud Storage.
        
        Args:
            blob_name: Nombre del blob en el bucket
            local_path: Ruta local donde guardar el archivo
            
        Returns:
            bool: True si la descarga fue exitosa
        """
        if not self.is_connected:
            await self.connect()
        
        if not self.bucket:
            raise RuntimeError("No hay bucket configurado")
        
        try:
            loop = asyncio.get_event_loop()
            blob = self.bucket.blob(blob_name)
            
            # Crear directorio local si no existe
            local_file_path = Path(local_path)
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            def _download_blob():
                # Verificar que el blob existe antes de descargar
                if not blob.exists():
                    raise FileNotFoundError(f"Blob no encontrado: {blob_name}")
                blob.download_to_filename(str(local_file_path))
                return True
            
            await loop.run_in_executor(None, _download_blob)
            
            logger.info(f"📥 Archivo descargado exitosamente: {blob_name} → {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error descargando archivo de Cloud Storage: {e}")
            return False
    
    async def list_blobs(self, prefix: Optional[str] = None, extensions: Optional[List[str]] = None) -> List[str]:
        """
        Lista los blobs en el bucket.
        
        Args:
            prefix: Filtro de prefijo para los nombres de los blobs
            extensions: Lista de extensiones permitidas
            
        Returns:
            List[str]: Lista de nombres de blobs
        """
        if not self.is_connected:
            await self.connect()
        
        if not self.bucket:
            raise RuntimeError("No hay bucket configurado")
        
        try:
            loop = asyncio.get_event_loop()
            
            def _list_blobs():
                blobs = list(self.bucket.list_blobs(prefix=prefix))
                return blobs
            
            blobs = await loop.run_in_executor(None, _list_blobs)
            
            blob_names = []
            for blob in blobs:
                # Filtrar por extensiones si se especifican
                if extensions and not any(blob.name.lower().endswith(ext) for ext in extensions):
                    continue
                blob_names.append(blob.name)
            
            logger.info(f"📂 Encontrados {len(blob_names)} archivos en Cloud Storage")
            return blob_names
            
        except Exception as e:
            logger.error(f"❌ Error listando archivos de Cloud Storage: {e}")
            return []