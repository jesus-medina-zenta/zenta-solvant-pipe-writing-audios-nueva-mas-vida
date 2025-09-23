"""
Pipeline para transferir archivos de audio desde Google Cloud Storage a SFTP con conversión.
"""
import asyncio
import tempfile
import os
import shutil
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path
from pydub import AudioSegment

from .config import get_cloud_storage_config, get_pipeline_config, get_sftp_config
from .utils.logger import get_logger
from .services.gcs_service import CloudStorageService
from .services.sftp_service import SFTPService

logger = get_logger(__name__)


class Pipeline:
    """
    Pipeline para transferir y convertir archivos de audio desde GCS a SFTP.
    """

    def __init__(self, audio_filter: Optional[Dict[str, Any]] = None, convert_to_wav: Optional[bool] = None, delete_after_upload: Optional[bool] = None):
        """
        Inicializa el pipeline.
        
        Args:
            audio_filter: Filtros para archivos de audio
            convert_to_wav: Si True, convierte todos los archivos a WAV
            delete_after_upload: Si True, borra archivos del bucket después de subir exitosamente
        """
        # Configuraciones
        self.gcs_config = get_cloud_storage_config()
        self.sftp_config = get_sftp_config()
        self.pipeline_config = get_pipeline_config()
        
        # Servicios
        self.gcs_service = CloudStorageService(
            project_id=self.gcs_config.project_id,
            bucket_name=self.gcs_config.bucket_name,
        )
        self.sftp_service = SFTPService(self.sftp_config)
        
        # Configuración de conversión
        self.convert_to_wav = self.pipeline_config.convert_to_wav
        self.delete_after_upload = self.pipeline_config.delete_after_upload

        # Filtros para archivos de audio
        self.audio_filter = audio_filter or {
            "prefix": "audios/",
            "extensions": [".mp3"]
        }
        
        # Directorio temporal para archivos
        self.temp_dir = tempfile.mkdtemp(prefix="audio_pipeline_")
        
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
    async def run(self) -> bool:
        """
        Ejecuta el pipeline completo: GCS → Local → Convert → SFTP.
        
        Returns:
            bool: True si fue exitoso
        """
        self.start_time = datetime.now(timezone.utc)
        logger.info("🎵 Iniciando pipeline de transferencia y conversión de audio")
        logger.info(f"📂 GCS Bucket: {self.gcs_config.bucket_name}")
        logger.info(f"🌐 SFTP Host: {self.sftp_config.host}")
        logger.info(f"🔄 Conversión a WAV: {'Activada' if self.convert_to_wav else 'Desactivada'}")

        phase_time = {}
        
        try:
            # Paso 1: Extract - Obtener lista de archivos de audio desde GCS
            extract_start = datetime.now(timezone.utc)
            logger.info("📊 Extrayendo lista de archivos de audio desde GCS...")
            audio_files = await self.extract_audio_files()
            extract_end = datetime.now(timezone.utc)
            phase_time['extract'] = (extract_end - extract_start).total_seconds()
            logger.info(f"🎵 Encontrados {len(audio_files)} archivos de audio")

            if not audio_files:
                logger.warning("⚠️ No se encontraron archivos de audio para transferir")
                return True
            
            # Paso 2: Download - Descargar archivos localmente
            download_start = datetime.now(timezone.utc)
            logger.info("📥 Descargando archivos de audio localmente...")
            downloaded_files = await self.download_audio_files(audio_files)
            download_end = datetime.now(timezone.utc)
            logger.info(f"📥 Descargados {len(downloaded_files)} archivos")
            phase_time["download"] = (download_end - download_start).total_seconds()

            # Paso 3: Transform - Convertir archivos si es necesario
            if self.convert_to_wav:
                convert_start = datetime.now(timezone.utc)
                logger.info("🔄 Convirtiendo archivos a formato WAV...")
                converted_files = await self.convert_audio_files(downloaded_files)
                convert_end = datetime.now(timezone.utc)
                logger.info(f"🔄 Convertidos {len(converted_files)} archivos")
                phase_time["convert"] = (convert_end - convert_start).total_seconds()
                files_to_upload = converted_files
            else:
                phase_time['convert'] = 0.0
                files_to_upload = downloaded_files
            
            # Paso 4: Load - Subir archivos a SFTP
            load_start = datetime.now(timezone.utc)
            logger.info("📤 Subiendo archivos a SFTP...")
            success = await self.upload_to_sftp(files_to_upload)
            load_end = datetime.now(timezone.utc)
            phase_time['load'] = (load_end - load_start).total_seconds()

            # Paso 5: Delete - Borrar archivos procesados del bucket (si está activado)
            if success and self.delete_after_upload:
                delete_start = datetime.now(timezone.utc)
                logger.info("🗑️ Borrando archivos procesados del bucket...")
                delete_result = await self.delete_processed_files(files_to_upload)
                delete_end = datetime.now(timezone.utc)
                phase_time['delete'] = (delete_end - delete_start).total_seconds()
                
                if delete_result:
                    logger.info(f"🗑️ Borrados {len(files_to_upload)} archivos del bucket")
                else:
                    logger.warning("⚠️ Algunos archivos no se pudieron borrar del bucket")
            else:
                phase_time['delete'] = 0.0

            # Estadísticas finales del pipeline
            total_duration = sum(phase_time.values())
            logger.info("🏁 ===== RESUMEN DEL PIPELINE =====")
            logger.info(f"   📊 Extracción: {phase_time['extract']:.2f}s ({phase_time['extract']/total_duration*100:.1f}%)")
            logger.info(f"   📥 Descarga: {phase_time['download']:.2f}s ({phase_time['download']/total_duration*100:.1f}%)")
            logger.info(f"   🔄 Conversión: {phase_time['convert']:.2f}s ({phase_time['convert']/total_duration*100:.1f}%)")
            logger.info(f"   📤 Subida: {phase_time['load']:.2f}s ({phase_time['load']/total_duration*100:.1f}%)")
            if phase_time['delete'] > 0:
                logger.info(f"   🗑️ Borrado: {phase_time['delete']:.2f}s ({phase_time['delete']/total_duration*100:.1f}%)")
            logger.info(f"   ⏱️ Total: {total_duration:.2f}s")
            logger.info("==================================")
            
            if success:
                logger.info("🎉 Pipeline completado exitosamente")
                return True
            else:
                logger.error("❌ Error en la transferencia a SFTP")
                return False
                
        except Exception as e:
            logger.exception(f"💥 Error en el pipeline: {e}")
            return False
        finally:
            await self.cleanup()
            self.end_time = datetime.now(timezone.utc)
            if self.start_time:
                duration = (self.end_time - self.start_time).total_seconds()
                logger.info(f"⏱️ Pipeline finalizado. Duración: {duration:.2f} segundos")
    
    async def extract_audio_files(self) -> List[Dict[str, Any]]:
        """
        Extrae metadatos de archivos de audio desde GCS.
        """
        try:
            # Crear filtros con extensiones de audio
            filters = {
                "prefix": self.audio_filter.get("prefix"),
                "extensions": self.audio_filter.get("extensions", [])
            }
            
            audio_files = await self.gcs_service.extract(filters)
            logger.info(f"🎵 Filtrados {len(audio_files)} archivos de audio")
            return audio_files

        except Exception as e:
            logger.error(f"❌ Error en la extracción: {e}")
            return []
    
    async def download_audio_files(self, audio_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Descarga archivos de audio desde GCS a directorio temporal.
        """
        downloaded_files = []
        
        for file_info in audio_files:
            try:
                blob_name = file_info.get("name")
                if not blob_name:
                    continue
                
                # Crear ruta local preservando estructura
                local_filename = blob_name.replace("/", "_")
                local_path = os.path.join(self.temp_dir, "original", local_filename)
                
                # Crear directorio si no existe
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Descargar archivo
                success = await self.gcs_service.download_file(blob_name, local_path)
                
                if success:
                    downloaded_files.append({
                        "blob_name": blob_name,
                        "local_path": local_path,
                        "original_extension": Path(blob_name).suffix.lower(),
                        "size": file_info.get("size", 0),
                        "content_type": file_info.get("content_type", "audio/mpeg")
                    })
                    logger.info(f"📥 Descargado: {blob_name}")
                else:
                    logger.error(f"❌ Error descargando: {blob_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error procesando archivo {file_info}: {e}")
        
        return downloaded_files
    
    async def convert_audio_files(self, downloaded_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convierte archivos de audio a formato WAV.
        
        Args:
            downloaded_files: Lista de archivos descargados
            
        Returns:
            List[Dict[str, Any]]: Lista de archivos convertidos
        """
        converted_files = []
        conversion_stats = {
            "success": 0, 
            "failed": 0, 
            "skipped": 0,
            "total_time": 0.0,
            "avg_time_per_file": 0.0,
            "times": []
            }
        
        # Tiempo de inicio total
        conversion_start_time = datetime.now(timezone.utc)
        logger.info(f"🔄 Iniciando conversión de {len(downloaded_files)} archivos...")
        
        for i, file_info in enumerate(downloaded_files, 1):
            # Tiempo de inicio para este archivo
            file_start_time = datetime.now(timezone.utc)
            
            try:
                local_path = file_info.get("local_path")
                original_extension = file_info.get("original_extension", "")
                blob_name = file_info.get("blob_name")
                original_size = file_info.get("size", 0)
                
                if not local_path or not os.path.exists(local_path):
                    logger.warning(f"⚠️ [{i}/{len(downloaded_files)}] Archivo no encontrado para conversión: {local_path}")
                    conversion_stats["failed"] += 1
                    continue
                
                # Si ya es WAV, no convertir
                if original_extension == ".wav":
                    file_end_time = datetime.now(timezone.utc)
                    skip_duration = (file_end_time - file_start_time).total_seconds()
                    
                    logger.info(f"⏭️ [{i}/{len(downloaded_files)}] Archivo ya es WAV, saltando conversión: {blob_name} ({skip_duration:.2f}s)")
                    converted_files.append({
                        **file_info,
                        "converted_path": local_path,
                        "converted_extension": ".wav",
                        "was_converted": False,
                        "conversion_time": skip_duration,
                        "conversion_status": "skipped"
                    })
                    conversion_stats["skipped"] += 1
                    conversion_stats["times"].append(skip_duration)
                    continue
                
                # Crear ruta para archivo convertido
                converted_filename = Path(blob_name).stem + ".wav"
                converted_path = os.path.join(
                    self.temp_dir, 
                    "converted", 
                    converted_filename.replace("/", "_")
                )
                
                # Crear directorio si no existe
                os.makedirs(os.path.dirname(converted_path), exist_ok=True)
                
                # Realizar conversión con tracking de tiempo
                logger.info(f"🔄 [{i}/{len(downloaded_files)}] Convirtiendo: {blob_name} ({original_size:,} bytes)")
                
                success = await self._convert_to_wav(local_path, converted_path, original_extension)
                
                # Calcular tiempo transcurrido
                file_end_time = datetime.now(timezone.utc)
                conversion_duration = (file_end_time - file_start_time).total_seconds()
                
                if success:
                    # Obtener tamaño del archivo convertido
                    converted_size = os.path.getsize(converted_path) if os.path.exists(converted_path) else 0
                    compression_ratio = (converted_size / original_size * 100) if original_size > 0 else 0
                    
                    converted_files.append({
                        **file_info,
                        "converted_path": converted_path,
                        "converted_extension": ".wav",
                        "was_converted": True,
                        "original_size": original_size,
                        "converted_size": converted_size,
                        "compression_ratio": compression_ratio,
                        "conversion_time": conversion_duration,
                        "conversion_status": "success"
                    })
                    conversion_stats["success"] += 1
                    conversion_stats["times"].append(conversion_duration)
                    
                    logger.info(f"✅ [{i}/{len(downloaded_files)}] Convertido: {blob_name} → WAV")
                    logger.info(f"   ⏱️ Tiempo: {conversion_duration:.2f}s")
                    logger.info(f"   📊 Tamaño: {original_size:,} → {converted_size:,} bytes ({compression_ratio:.1f}%)")
                    
                else:
                    logger.error(f"❌ [{i}/{len(downloaded_files)}] Error convirtiendo: {blob_name} ({conversion_duration:.2f}s)")
                    conversion_stats["failed"] += 1
                    conversion_stats["times"].append(conversion_duration)
                    
            except Exception as e:
                file_end_time = datetime.now(timezone.utc)
                error_duration = (file_end_time - file_start_time).total_seconds()
                
                logger.error(f"❌ [{i}/{len(downloaded_files)}] Error procesando conversión {file_info}: {e} ({error_duration:.2f}s)")
                conversion_stats["failed"] += 1
                conversion_stats["times"].append(error_duration)
        
        # Calcular estadísticas finales
        conversion_end_time = datetime.now(timezone.utc)
        total_conversion_time = (conversion_end_time - conversion_start_time).total_seconds()
        conversion_stats["total_time"] = total_conversion_time
        
        if conversion_stats["times"]:
            conversion_stats["avg_time_per_file"] = sum(conversion_stats["times"]) / len(conversion_stats["times"])
            conversion_stats["min_time"] = min(conversion_stats["times"])
            conversion_stats["max_time"] = max(conversion_stats["times"])
        
        # Log de estadísticas finales
        logger.info("📊 ===== ESTADÍSTICAS DE CONVERSIÓN =====")
        logger.info(f"   📁 Total archivos: {len(downloaded_files)}")
        logger.info(f"   ✅ Exitosos: {conversion_stats['success']}")
        logger.info(f"   ⏭️ Saltados: {conversion_stats['skipped']}")
        logger.info(f"   ❌ Fallos: {conversion_stats['failed']}")
        logger.info(f"   ⏱️ Tiempo total: {total_conversion_time:.2f}s")
        logger.info(f"   📊 Tiempo promedio por archivo: {conversion_stats['avg_time_per_file']:.2f}s")
        if conversion_stats["times"]:
            logger.info(f"   ⚡ Archivo más rápido: {conversion_stats['min_time']:.2f}s")
            logger.info(f"   🐌 Archivo más lento: {conversion_stats['max_time']:.2f}s")
        logger.info("=========================================")
        
        return converted_files
    
    async def _convert_to_wav(self, input_path: str, output_path: str, original_extension: str) -> bool:
            """
            Convierte un archivo de audio a WAV usando pydub con tracking de tiempo interno.
            
            Args:
                input_path: Ruta del archivo original
                output_path: Ruta del archivo WAV convertido
                original_extension: Extensión original del archivo
                
            Returns:
                bool: True si la conversión fue exitosa
            """
            try:
                loop = asyncio.get_event_loop()
                
                # Tiempo de inicio para operaciones internas
                load_start = datetime.now(timezone.utc)
                
                def _convert():
                    # Determinar formato de entrada
                    format_map = {
                        ".mp3": "mp3",
                        ".m4a": "m4a", 
                        ".flac": "flac",
                        ".aac": "aac",
                        ".ogg": "ogg"
                    }
                    
                    input_format = format_map.get(original_extension, "mp3")
                    
                    # Cargar archivo de audio
                    if original_extension == ".mp3":
                        audio = AudioSegment.from_mp3(input_path)
                    else:
                        audio = AudioSegment.from_file(input_path, format=input_format)
                    
                    load_end = datetime.now(timezone.utc)
                    load_time = (load_end - load_start).total_seconds()
                    
                    # Configurar parámetros de salida WAV
                    process_start = datetime.now(timezone.utc)
                    audio = audio.set_frame_rate(44100)  # 44.1 kHz
                    audio = audio.set_channels(2)        # Estéreo
                    audio = audio.set_sample_width(2)    # 16-bit
                    
                    process_end = datetime.now(timezone.utc)
                    process_time = (process_end - process_start).total_seconds()
                    
                    # Exportar como WAV
                    export_start = datetime.now(timezone.utc)
                    audio.export(output_path, format="wav")
                    export_end = datetime.now(timezone.utc)
                    export_time = (export_end - export_start).total_seconds()
                    
                    # Verificar que el archivo se creó correctamente
                    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                        raise Exception("Archivo WAV no se creó correctamente")
                    
                    # Log de tiempos internos (solo en modo debug)
                    logger.debug(f"   🔍 Tiempos internos - Carga: {load_time:.2f}s, Proceso: {process_time:.2f}s, Exportar: {export_time:.2f}s")
                    
                    return True
                
                # Ejecutar conversión en thread pool
                result = await loop.run_in_executor(None, _convert)
                return result
                
            except Exception as e:
                logger.error(f"❌ Error en conversión de audio: {e}")
                return False

    
    async def upload_to_sftp(self, files_to_upload: List[Dict[str, Any]]) -> bool:
        """
        Sube archivos (convertidos o originales) al servidor SFTP usando ruta fija.
        """
        try:
            sftp_upload_data = []
            
            for file_info in files_to_upload:
                # Usar archivo convertido si existe, sino el original
                local_path = file_info.get("converted_path") or file_info.get("local_path")
                blob_name = file_info.get("blob_name")
                
                if not local_path or not blob_name:
                    continue
                
                # Crear nombre de archivo para SFTP
                if file_info.get("was_converted"):
                    # Si fue convertido, cambiar extensión a .wav
                    remote_filename = Path(blob_name).stem + ".wav"
                else:
                    # Si no fue convertido, mantener nombre original
                    remote_filename = Path(blob_name).name
                
                # USAR RUTA FIJA Y SIMPLE - sin crear subdirectorios
                remote_path = f"{self.sftp_config.upload_path}/{remote_filename}"
                
                sftp_upload_data.append({
                    "local_path": local_path,
                    "remote_path": remote_path
                })
                
                logger.info(f"📋 Preparado para subir: {remote_filename}")
            
            logger.info(f"🔄 Subiendo {len(sftp_upload_data)} archivos a {self.sftp_config.upload_path}")
            
            # Subir archivos
            success = await self.sftp_service.load(sftp_upload_data)
            return success
            
        except Exception as e:
            logger.error(f"❌ Error preparando subida a SFTP: {e}")
            return False
    
    async def cleanup(self) -> None:
        """Limpia archivos temporales y cierra conexiones."""
        # Primero cerrar conexiones
        try:
            await self.gcs_service.disconnect()
            await self.sftp_service.disconnect()  # CORREGIDO: Ahora es asíncrono
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando conexiones: {e}")
        
        # Luego limpiar archivos temporales
        try:
            if os.path.exists(self.temp_dir):
                # Dar tiempo para que se liberen los archivos
                await asyncio.sleep(0.5)
                shutil.rmtree(self.temp_dir)
                logger.info(f"🧹 Limpieza completada: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"⚠️ Error en limpieza: {e}")

    async def delete_processed_files(self, files_to_delete: List[Dict[str, Any]]) -> bool:
        """
        Borra archivos procesados exitosamente del bucket GCS.
        
        Args:
            files_to_delete: Lista de archivos que fueron subidos exitosamente
            
        Returns:
            bool: True si todos los archivos fueron borrados exitosamente
        """
        if not files_to_delete:
            logger.info("ℹ️ No hay archivos para borrar del bucket")
            return True
        
        if not self.delete_after_upload:
            logger.info("ℹ️ Borrado de archivos desactivado")
            return True
        
        try:
            deletion_stats = {
                "success": 0,
                "failed": 0,
                "total": len(files_to_delete)
            }
            
            logger.info(f"🗑️ Iniciando borrado de {len(files_to_delete)} archivos del bucket...")
            
            for i, file_info in enumerate(files_to_delete, 1):
                try:
                    blob_name = file_info.get("blob_name")
                    if not blob_name:
                        logger.warning(f"⚠️ [{i}/{len(files_to_delete)}] Nombre de blob no encontrado en file_info")
                        deletion_stats["failed"] += 1
                        continue
                    
                    logger.info(f"🗑️ [{i}/{len(files_to_delete)}] Borrando: {blob_name}")
                    
                    # Borrar archivo del bucket
                    success = await self.gcs_service.delete_file(blob_name)
                    
                    if success:
                        deletion_stats["success"] += 1
                        logger.info(f"✅ [{i}/{len(files_to_delete)}] Borrado: {blob_name}")
                    else:
                        deletion_stats["failed"] += 1
                        logger.error(f"❌ [{i}/{len(files_to_delete)}] Error borrando: {blob_name}")
                        
                except Exception as e:
                    deletion_stats["failed"] += 1
                    logger.error(f"❌ [{i}/{len(files_to_delete)}] Error procesando borrado de {file_info}: {e}")
            
            # Log de estadísticas de borrado
            logger.info("📊 ===== ESTADÍSTICAS DE BORRADO =====")
            logger.info(f"   📁 Total archivos: {deletion_stats['total']}")
            logger.info(f"   ✅ Borrados exitosamente: {deletion_stats['success']}")
            logger.info(f"   ❌ Fallos: {deletion_stats['failed']}")
            logger.info(f"   📊 Tasa de éxito: {deletion_stats['success']/deletion_stats['total']*100:.1f}%")
            logger.info("=====================================")
            
            # Retornar True solo si todos fueron borrados exitosamente
            return deletion_stats["failed"] == 0
            
        except Exception as e:
            logger.error(f"❌ Error general en borrado de archivos: {e}")
            return False