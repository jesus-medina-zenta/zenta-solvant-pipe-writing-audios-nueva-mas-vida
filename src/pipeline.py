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
from zoneinfo import ZoneInfo
from pydub import AudioSegment

from src.models.data_models import ProcessingStats
from src.models.firestore_records import AudioProcessingTask, AudioStatus, AudioStatusRecord
from src.services.audio_service import AudioAnalyzerService
from src.services.filename_service import FilenameService
from src.services.firestore_service import FirestoreService

from src.config import get_cloud_storage_config, get_firestore_config, get_pipeline_config, get_sftp_config
from src.utils.logger import get_logger
from src.services.gcs_service import CloudStorageService
from src.services.sftp_service import SFTPService

logger = get_logger(__name__)

# Un MP3 de una llamada que se cortó al iniciar pesa ~45 bytes (solo la etiqueta ID3)
MIN_AUDIO_BYTES = 1024
MIN_AUDIO_SECONDS = 1.0
# Intentos por audio (entre ejecuciones) antes de dejarlo en FAILED
MAX_ATTEMPTS = 3
# Reintentos de ffmpeg dentro de una misma ejecución (fallas transitorias)
CONVERSION_ATTEMPTS = 3
# Un audio en PROCESSING por más de esto se considera abandonado
STALE_PROCESSING_SECONDS = 30 * 60
# La carpeta del SFTP es el día de subida en Chile, formato DDMMYYYY
SFTP_FOLDER_TZ = ZoneInfo("America/Santiago")


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
        self.firestore_service = FirestoreService()
        self.audio_service = AudioAnalyzerService()
        
        # Configuración de procesamiento
        self.batch_size = getattr(self.pipeline_config, 'batch_size', 10)
        self.enable_status_updates = getattr(self.pipeline_config, 'enable_status_updates', True)
        self.convert_to_wav = convert_to_wav if convert_to_wav is not None else getattr(self.pipeline_config, 'convert_to_wav', True)
        self.delete_after_upload = delete_after_upload if delete_after_upload is not None else getattr(self.pipeline_config, 'delete_after_upload', True)

        # Filtros para archivos de audio
        self.audio_filter = audio_filter or {
            "prefix": self.gcs_config.audio_prefix,
            "extensions": [".mp3"]
        }
        
        # Directorio temporal y estadísticas
        self.temp_dir = tempfile.mkdtemp(prefix="audio_pipeline_")
        self.stats = ProcessingStats()
        
    async def run(self) -> bool:
        """
        Ejecuta el pipeline completo basado en estados de Firestore.
        
        Returns:
            bool: True si fue exitoso
        """
        logger.info("🎵 Iniciando Audio Pipeline - Status-Driven")
        logger.info(f"📂 GCS Bucket: {self.gcs_config.bucket_name}")
        logger.info(f"🌐 SFTP Host: {self.sftp_config.host}")
        logger.info(f"📊 Tamaño de lote: {self.batch_size}")
        logger.info(f"🔄 Conversión a WAV: {'Activada' if self.convert_to_wav else 'Desactivada'}")
        
        try:
            self.stats.start_time = datetime.now(timezone.utc)

            # Paso 0: Recuperar audios que quedaron colgados en PROCESSING
            requeued = self.firestore_service.requeue_stale_processing(STALE_PROCESSING_SECONDS)
            if requeued:
                logger.info(f"♻️ {requeued} audios en PROCESSING reencolados")

            # Paso 1: Obtener audios pendientes desde Firestore
            pending_audios = self.firestore_service._query_pending_audios_sync(limit=self.batch_size * 10)
            if not pending_audios:
                logger.info("ℹ️ No hay audios pendientes para procesar")
                return True
            
            self.stats.total_files = len(pending_audios)
            logger.info(f"🎵 Encontrados {len(pending_audios)} audios pendientes")

            # Paso 2: Procesar en lotes
            successfully_processed = []
            skipped_tasks = []
            failed_tasks = []

            for i in range(0, len(pending_audios), self.batch_size):
                batch = pending_audios[i:i + self.batch_size]
                logger.info(f"📦 Procesando lote {i//self.batch_size + 1}: {len(batch)} audios")

                batch_results = await self._process_audio_batch(batch)

                for task in batch_results:
                    if task.processing_status == "completed":
                        successfully_processed.append(task)
                    elif task.processing_status == "skipped":
                        skipped_tasks.append(task)
                    else:
                        failed_tasks.append(task)

            # Paso 3: Log estadísticas finales
            self._log_final_statistics(successfully_processed, failed_tasks, skipped_tasks)

            # Cada audio fallido ya quedó reencolado (o en FAILED tras MAX_ATTEMPTS),
            # así que un fallo puntual no amerita que Cloud Run relance el job completo.
            # Solo se reporta error si nada salió: probable caída de SFTP/GCS/ffmpeg.
            if failed_tasks and not successfully_processed and not skipped_tasks:
                logger.error(f"💥 Fallaron los {len(failed_tasks)} audios procesados, se reportará error")
                return False

            if failed_tasks:
                logger.warning(f"⚠️ Pipeline completado con {len(failed_tasks)} audios reencolados para reintento")
            else:
                logger.info("🎉 Pipeline completado exitosamente")
            return True
            
        except Exception as e:
            logger.exception(f"💥 Error crítico en pipeline: {e}")
            return False
        finally:
            await self._cleanup()
            self.stats.finish()
    
    async def _process_audio_batch(self, audio_records: List[AudioStatusRecord]) -> List[AudioProcessingTask]:
        """
        Procesa un lote de audios de forma concurrente.
        """
        # Crear tareas de procesamiento
        tasks = []
        for record in audio_records:
            task = AudioProcessingTask(
                conversation_id=record.id_conversacion,
                audio_status=record,
                document_id=getattr(record, 'document_id', None)
            )
            tasks.append(task)
        
        logger.info(f"📋 Creadas {len(tasks)} tareas de procesamiento")
        
        # Marcar como "en procesamiento"
        if self.enable_status_updates:
            await self._mark_batch_as_processing(tasks)

        # Procesar cada tarea de forma concurrente (máximo 1 a la vez)
        semaphore = asyncio.Semaphore(1)
        
        async def process_with_semaphore(task):
            async with semaphore:
                return await self._process_single_audio_task(task)
        
        # Ejecutar todas las tareas
        results = await asyncio.gather(
            *[process_with_semaphore(task) for task in tasks],
            return_exceptions=True
        )
        
        # Manejar resultados y excepciones
        processed_tasks = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                tasks[i].processing_status = "failed"
                tasks[i].error_message = str(result)
                logger.error(f"❌ Error procesando {tasks[i].conversation_id}: {result}")
                processed_tasks.append(tasks[i])
            else:
                processed_tasks.append(result)
        
        return processed_tasks

    async def _process_single_audio_task(self, task: AudioProcessingTask) -> AudioProcessingTask:
        """
        Procesa una sola tarea de audio de principio a fin.
        """
        start_time = datetime.now(timezone.utc)
        audio_duration_seconds = None
        try:
            logger.info(f"🎵 Procesando audio: {task.conversation_id}")
            
            # 1. Buscar archivo específico en bucket
            task.processing_status = "downloading"
            audio_file = await self._find_audio_file_in_bucket(task.conversation_id)
            
            if not audio_file:
                task.processing_status = "failed"
                task.error_message = f"Archivo no encontrado en bucket para {task.conversation_id}"
                self.stats.add_failed()

                if self.enable_status_updates:
                    self.firestore_service._update_audio_status_sync(
                        task.conversation_id,
                        AudioStatus.NOT_FOUND,
                        getattr(task.audio_status, 'document_id', None)
                    )
                logger.warning(f"⚠️ Archivo no encontrado para {task.conversation_id}, NOT FOUND registrado en Firestore")
                return task
            
            # 2. Descargar archivo
            blob_name = audio_file.get("name")
            local_filename = blob_name.replace("/", "_")
            local_path = os.path.join(self.temp_dir, "original", local_filename)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            download_success = await self.gcs_service.download_file(blob_name, local_path)
            if not download_success:
                return self._fail_task(task, f"Error descargando {blob_name}")
            
            task.local_path = local_path
            task.original_filename = blob_name
            self.stats.add_downloaded()
            
            original_format = Path(blob_name).suffix.lower().replace(".", "")
            audio_metadata = self.audio_service.get_audio_metadata(local_path, original_format)

            audio_duration_seconds = audio_metadata.get("duration_seconds")

            logger.info(f"🎵 Audio analizado: {task.conversation_id}")
            logger.info(f"   📊 Duración: {audio_metadata.get('duration_formatted', 'N/A')}")
            logger.info(f"   📦 Tamaño: {audio_metadata.get('file_size_bytes', 0)} bytes")
            logger.info(f"   🔊 Formato: {audio_metadata.get('format', 'unknown')}")

            # Llamadas que se cortaron al iniciar dejan un MP3 sin audio: no hay nada que subir
            no_audio_reason = self._no_audio_reason(audio_metadata)
            if no_audio_reason:
                task.processing_status = "skipped"
                task.error_message = no_audio_reason
                self.stats.add_skipped()
                if self.enable_status_updates:
                    self.firestore_service._update_audio_status_sync(
                        task.conversation_id,
                        AudioStatus.NO_AUDIO,
                        getattr(task.audio_status, 'document_id', None)
                    )
                logger.warning(f"🔇 {task.conversation_id} sin audio real ({no_audio_reason}), marcado NO_AUDIO")
                return task

            if audio_duration_seconds is not None:
                logger.info(f"Actualizando duración en Firestore: {audio_duration_seconds} segundos")
                duration_updated = self.firestore_service.update_call_duration_audio(task.conversation_id, audio_duration_seconds)

                if duration_updated:
                    logger.info(f"✅ Duración actualizada exitosamente para {task.conversation_id}")
                else:
                    logger.warning(f"⚠️ Error actualizando duración para {task.conversation_id}")
            # 3. Obtener datos del usuario y construir nombre personalizado
            call_record = self.firestore_service._get_call_record_sync(task.conversation_id)
            if call_record:
                task.user_variables = call_record
                task.target_filename = FilenameService.build_filename_from_call_record(call_record)
                logger.info(f"📝 Nombre construido para {task.conversation_id}: {task.target_filename}")
            else:
                # Nombre de respaldo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                task.target_filename = f"PP_error_{timestamp}_{task.conversation_id}_unknown.wav"
                logger.warning(f"⚠️ Usando nombre de respaldo para {task.conversation_id}: {task.target_filename}")
            
            # 4. Convertir a WAV (si es necesario)
            if self.convert_to_wav:
                task.processing_status = "converting"
                original_extension = Path(blob_name).suffix.lower()
                
                if original_extension != ".wav":
                    converted_filename = Path(task.target_filename).stem + ".wav"
                    converted_path = os.path.join(self.temp_dir, "converted", converted_filename)
                    os.makedirs(os.path.dirname(converted_path), exist_ok=True)
                    
                    conversion_success = await self._convert_to_wav(local_path, converted_path, original_extension)
                    if conversion_success:
                        task.converted_path = converted_path
                        self.stats.add_converted()
                    else:
                        return self._fail_task(task, f"Error convirtiendo {blob_name}")
                else:
                    task.converted_path = local_path
                    self.stats.add_converted()
            
            # 5. Subir a SFTP (en una subcarpeta con la fecha del día de la subida)
            task.processing_status = "uploading"
            upload_date_folder = datetime.now(SFTP_FOLDER_TZ).strftime("%d%m%Y")
            upload_data = [{
                "local_path": task.converted_path or task.local_path,
                "remote_path": f"{self.sftp_config.upload_path}/{upload_date_folder}/{task.target_filename}",
                "target_filename": task.target_filename
            }]

            upload_success = await self.sftp_service.load(upload_data)
            if not upload_success:
                return self._fail_task(task, f"Error subiendo a SFTP: {task.target_filename}")
            
            self.stats.add_uploaded()
            
            # 6. Actualizar estado en Firestore
            if self.enable_status_updates:
                update_success = self.firestore_service._update_audio_status_sync(
                    task.conversation_id,
                    AudioStatus.UPLOADED_TO_SFTP,
                    getattr(task.audio_status, 'document_id', None)
                )
                if update_success:
                    self.stats.add_status_updated()
                else:
                    logger.warning(f"⚠️ Error actualizando estado para {task.conversation_id}")
            
            task.processing_status = "completed"
            logger.info(f"✅ Audio procesado exitosamente: {task.conversation_id} → {task.target_filename}")
            
            return task
            
        except Exception as e:
            logger.error(f"❌ Error procesando {task.conversation_id}: {e}")
            return self._fail_task(task, str(e))

    def _fail_task(self, task: AudioProcessingTask, error_message: str) -> AudioProcessingTask:
        """
        Marca la tarea como fallida y registra el intento en Firestore: vuelve a
        AUDIO_SAVED_IN_BUCKET si quedan intentos, o FAILED si se agotaron.
        """
        task.processing_status = "failed"
        task.error_message = error_message
        self.stats.add_failed()

        if self.enable_status_updates:
            self.firestore_service.register_failed_attempt(
                task.conversation_id,
                error_message,
                MAX_ATTEMPTS,
                getattr(task.audio_status, 'document_id', None)
            )
        return task

    @staticmethod
    def _no_audio_reason(audio_metadata: Dict[str, Any]) -> Optional[str]:
        """
        Retorna el motivo si el archivo no contiene audio real, o None si es válido.
        Una duración desconocida en un archivo de tamaño normal NO se considera vacío:
        puede ser una falla transitoria de ffmpeg y se deja seguir a la conversión.
        """
        file_size = audio_metadata.get("file_size_bytes") or 0
        if file_size < MIN_AUDIO_BYTES:
            return f"archivo de {file_size} bytes"

        duration = audio_metadata.get("duration_seconds")
        if duration is not None and duration < MIN_AUDIO_SECONDS:
            return f"duración de {duration:.2f}s"
        return None

    async def _mark_batch_as_processing(self, tasks: List[AudioProcessingTask]) -> None:
        """
        Marca un lote de tareas como 'PROCESSING' para evitar procesamiento concurrente.
        """
        try:
            for task in tasks:
                success = self.firestore_service._update_audio_status_sync(
                    task.conversation_id,
                    AudioStatus.PROCESSING,
                    getattr(task.audio_status, 'document_id', None)
                )
                if not success:
                    logger.warning(f"⚠️ Error marcando como PROCESSING: {task.conversation_id}")
            
            logger.info(f"🔄 Marcadas {len(tasks)} tareas como PROCESSING")
            
        except Exception as e:
            logger.error(f"❌ Error marcando lote como PROCESSING: {e}")

    async def _find_audio_file_in_bucket(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca el archivo de audio específico para un conversation_id en el bucket.
        """
        try:
            filters = {
                "prefix": self.gcs_config.audio_prefix,
                "extensions": [".mp3", ".wav", ".m4a"]
            }
            
            all_files = await self.gcs_service.extract(filters)
            
            # Buscar archivo que corresponda a este conversation_id
            for file_info in all_files:
                filename = file_info.get("name", "")
                
                # Extraer conversation_id del nombre del archivo
                file_conv_id = FilenameService.extract_conversation_id_from_filename(filename)
                
                if file_conv_id == conversation_id:
                    logger.info(f"📁 Archivo encontrado para {conversation_id}: {filename}")
                    return file_info
            
            logger.warning(f"⚠️ No se encontró archivo para {conversation_id}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error buscando archivo para {conversation_id}: {e}")
            return None

    async def _convert_to_wav(self, input_path: str, output_path: str, original_extension: str) -> bool:
        """
        Convierte un archivo de audio a WAV usando pydub.
        """
        try:
            loop = asyncio.get_event_loop()
            
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
                
                # Configurar parámetros de salida WAV
                audio = audio.set_frame_rate(44100)  # 44.1 kHz
                audio = audio.set_channels(2)        # Estéreo
                audio = audio.set_sample_width(2)    # 16-bit
                
                # Exportar como WAV
                audio.export(output_path, format="wav")
                
                # Verificar que el archivo se creó correctamente
                if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                    raise Exception("Archivo WAV no se creó correctamente")
                
                return True
            
            # ffmpeg a veces aborta de forma transitoria en Cloud Run (ej. código -6)
            # con archivos sanos, así que se reintenta antes de dar el audio por fallido.
            for attempt in range(1, CONVERSION_ATTEMPTS + 1):
                try:
                    # Ejecutar conversión en thread pool
                    return await loop.run_in_executor(None, _convert)
                except Exception as e:
                    logger.error(f"❌ Error en conversión de audio (intento {attempt}/{CONVERSION_ATTEMPTS}): {e}")
                    if attempt < CONVERSION_ATTEMPTS:
                        await asyncio.sleep(2 * attempt)
            return False

        except Exception as e:
            logger.error(f"❌ Error en conversión de audio: {e}")
            return False

    def _log_final_statistics(self, successful_tasks: List[AudioProcessingTask], failed_tasks: List[AudioProcessingTask],
                              skipped_tasks: Optional[List[AudioProcessingTask]] = None) -> None:
        """
        Registra estadísticas finales del procesamiento.
        La tasa de éxito se calcula solo sobre audios reales (sin contar los NO_AUDIO).
        """
        skipped_tasks = skipped_tasks or []
        real_tasks = len(successful_tasks) + len(failed_tasks)
        success_rate = (len(successful_tasks) / real_tasks * 100) if real_tasks > 0 else 0

        logger.info("📊 ===== ESTADÍSTICAS FINALES DEL PIPELINE =====")
        logger.info(f"   📁 Total audios procesados: {real_tasks + len(skipped_tasks)}")
        logger.info(f"   ✅ Exitosos: {len(successful_tasks)}")
        logger.info(f"   🔇 Sin audio (NO_AUDIO): {len(skipped_tasks)}")
        logger.info(f"   ❌ Fallos: {len(failed_tasks)}")
        logger.info(f"   📊 Tasa de éxito: {success_rate:.1f}%")
        logger.info(f"   📥 Archivos descargados: {self.stats.downloaded_files}")
        logger.info(f"   🔄 Archivos convertidos: {self.stats.converted_files}")
        logger.info(f"   📤 Archivos subidos: {self.stats.uploaded_files}")
        logger.info(f"   🔄 Estados actualizados: {self.stats.updated_status_files}")
        logger.info(f"   ⏱️ Tiempo total: {self.stats.processing_time_seconds:.2f}s")
        logger.info("============================================")
        
        # Log errores específicos
        if failed_tasks:
            logger.error("❌ ERRORES DETALLADOS:")
            for task in failed_tasks:
                logger.error(f"   🔴 {task.conversation_id}: {task.error_message}")

    async def _cleanup(self) -> None:
        """Limpia archivos temporales y cierra conexiones."""
        try:
            await self.gcs_service.disconnect()
            await self.sftp_service.disconnect()
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando conexiones: {e}")
        
        try:
            if os.path.exists(self.temp_dir):
                await asyncio.sleep(0.5)  # Dar tiempo para liberar archivos
                shutil.rmtree(self.temp_dir)
                logger.info(f"🧹 Limpieza completada: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"⚠️ Error en limpieza: {e}")