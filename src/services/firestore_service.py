import asyncio
from typing import Optional, List
from google.cloud import firestore
from src.models.firestore_records import AudioStatus, AudioStatusRecord, DynamicVariables
from src.models.data_models import DataRecord
from src.models.log_record import LogRecord
from src.models.firestore_records import DynamicVariables
from src.utils.logger import get_logger
from src.config import get_config, get_firestore_config
from src.enums.status_enum import StatusType
import os
from dotenv import load_dotenv
load_dotenv()

logger = get_logger(__name__)

class FirestoreService:
    def __init__(self):
        self.client: Optional[firestore.Client] = None
        self.firestore_config = get_firestore_config()
        self.project_id = self.firestore_config.project_id
        self.audio_status_collection = self.firestore_config.audios_status_collection
        self.registros_llamadas_collection = self.firestore_config.registros_llamadas_collection
        self.database = self.firestore_config.database
        self.is_connected = False

    def connect(self) -> bool:
        if self.is_connected and self.client:
            return True
            
        try:
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                logger.warning("GOOGLE_APPLICATION_CREDENTIALS no está configurado.")
            
            logger.info("Conectando a Firestore")
            self.client = firestore.Client(
                project=self.project_id, 
                database=self.database
            )
            
            # Validar conexión real
            self.client.collection(self.registros_llamadas_collection).limit(1).get()
            
            self.is_connected = True
            logger.info("Conexión establecida exitosamente")
            return True
        except Exception as e:
            logger.error("Error conectando a Firestore: %s", e, exc_info=True)
            self.is_connected = False
            return False

    def disconnect(self) -> None:
        if self.client:
            self.client = None
            self.is_connected = False
            logger.info("Conexión a Firestore cerrada")

    def _query_pending_audios_sync(self, limit: int = 100) -> List[AudioStatusRecord]:
        """Consulta síncrona de audios pendientes."""
        if not self.is_connected:
            self.connect()
        
        try:
            collection_ref = self.client.collection(self.audio_status_collection)
            query = collection_ref.where('status', '==', AudioStatus.AUDIO_SAVED_IN_BUCKET.value).where('agent_id', '==', get_config().agent_id).limit(limit)
            
            docs = query.stream()

            records = []
            for doc in docs:
                try:
                    data = doc.to_dict()
                    record = AudioStatusRecord(**data)
                    records.append(record)
                except Exception as e:
                    logger.warning(f"⚠️ Error procesando documento {doc.id}: {e}")
                    continue
            return records
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo audios pendientes: {e}")
            return []

    def _get_call_record_sync(self, conversation_id: str):
        """
        Obtiene el registro de llamada desde la colección registros_llamadas usando conversation_id.
        """
        if not self.is_connected:
            self.connect()
        
        try:
            logger.info(f"🔍 Buscando registro de llamada para conversation_id: {conversation_id}")
            
            collection_ref = self.client.collection(self.registros_llamadas_collection)
            
            # Buscar por el campo conversation_id
            query = collection_ref.where('conversation_id', '==', conversation_id).limit(1)
            docs = list(query.stream())
            
            if docs:
                doc = docs[0]
                data = doc.to_dict()
                logger.info(f"✅ Registro de llamada encontrado para {conversation_id}")
                
                # Aplanar la estructura: mover dynamic_variables al nivel raíz
                if 'dynamic_variables' in data and isinstance(data['dynamic_variables'], dict):
                    dynamic_vars = data['dynamic_variables']
                    # Mover campos de dynamic_variables al nivel raíz
                    for key, value in dynamic_vars.items():
                        data[key] = value
                    logger.debug(f"📊 Variables dinámicas integradas: {list(dynamic_vars.keys())}")
                
                # Log de campos clave para debug
                key_fields = ['conversation_id', 'rut', 'dv', 'name', 'paternal_surname', 'event_timestamp']
                for field in key_fields:
                    if field in data:
                        logger.debug(f"📝 {field}: {data[field]}")
                
                try:
                    record = DynamicVariables(**data)
                    logger.info(f"📝 Registro parseado exitosamente")
                    return record
                except Exception as parse_error:
                    logger.error(f"❌ Error parseando registro: {parse_error}")
                    logger.debug(f"📋 Datos disponibles: {list(data.keys())}")
                    return None
            else:
                logger.warning(f"⚠️ No se encontró registro de llamada para: {conversation_id}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo registro de llamada para {conversation_id}: {e}")
            return None

    def _update_audio_status_sync(self, conversation_id: str, new_status: AudioStatus, document_id: Optional[str] = None) -> bool:
        """Actualización síncrona de estado de audio."""
        if not self.is_connected:
            self.connect()
        
        try:
            import time
            
            if document_id:
                # Actualización directa si conocemos el document_id
                doc_ref = self.client.collection(self.audio_status_collection).document(document_id)
                doc_ref.update({
                    'status': new_status.value,
                    'update_at': int(time.time())
                })
                logger.info(f"✅ Estado actualizado para {conversation_id}: {new_status.value}")
                return True
            else:
                # Buscar documento por conversation_id
                collection_ref = self.client.collection(self.audio_status_collection)
                query = collection_ref.where('id_conversacion', '==', conversation_id).limit(1)
                docs = list(query.stream())
                
                if docs:
                    doc = docs[0]
                    doc.reference.update({
                        'status': new_status.value,
                        'update_at': int(time.time())
                    })
                    logger.info(f"✅ Estado actualizado para {conversation_id}: {new_status.value}")
                    return True
                else:
                    logger.warning(f"⚠️ No se encontró documento para actualizar: {conversation_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error actualizando estado para {conversation_id}: {e}")
            return False

    def update_log_status(self, log_id: str, status: StatusType, **kwargs) -> bool:
        """
        Actualiza el estado de un registro de log.
        
        Args:
            log_id: ID del log a actualizar
            status: Nuevo estado
            **kwargs: Campos adicionales a actualizar
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        try:
            doc_ref = self.client.collection(self.audio_status_collection).document(log_id)
            
            update_data = {
                "status": status.value,
                **kwargs
            }
            
            doc_ref.update(update_data)
            logger.info(f"✅ Log {log_id} actualizado a estado {status.value}")
            
            # Log adicional si hay errores
            if "errores" in kwargs and kwargs["errores"]:
                error_count = len(kwargs["errores"])
                logger.info(f"📝 Se registraron {error_count} errores en el log")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error actualizando log {log_id}: {e}")
            return False

    def save_transformed_records(self, records: list, log_id: str):
        """Guarda registros transformados en Firestore."""
        try:
            collection_ref = self.client.collection(self.registros_llamadas_collection)
            batch = self.client.batch()
            
            for idx, record in enumerate(records):
                if isinstance(record, DataRecord):
                    record_dict = record.model_dump(mode="json")
                else:
                    record_dict = record
                
                # Agregar referencia al log original
                record_dict["log_id"] = log_id
                record_dict["processed_at"] = firestore.SERVER_TIMESTAMP
                
                doc_id = f"{log_id}_{idx+1}"
                doc_ref = collection_ref.document(doc_id)
                batch.set(doc_ref, record_dict)
            
            batch.commit()
            logger.info(f"✅ {len(records)} registros guardados en la colección '{self.registros_llamadas_collection}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando registros transformados: {e}")
            return False

    def save_log_record(self, log: LogRecord):
        """Guarda o actualiza un registro de log."""
        try:
            doc_ref = self.client.collection(self.audio_status_collection).document(log.id)
            doc_ref.set(log.model_dump(mode="json"))
            logger.info(f"✅ Log guardado con id {log.id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error guardando log: {e}")
            return False
    def update_call_duration_audio(self, conversation_id: str, duration_seconds: float) -> bool:
        """
        Actualiza la duración del audio en el registro de llamada.
        Agrega el campo 'call_duration_secs_audio' en la raíz del documento.
        
        Args:
            conversation_id: ID de conversación
            duration_seconds: Duración del audio en segundos
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        if not self.is_connected:
            self.connect()
        
        try:
            logger.info(f"🔄 Actualizando duración de audio para: {conversation_id}")
            
            collection_ref = self.client.collection(self.registros_llamadas_collection)
            
            # Buscar documento por conversation_id
            query = collection_ref.where('conversation_id', '==', conversation_id).limit(1)
            docs = list(query.stream())
            
            if docs:
                doc = docs[0]
                doc_id = doc.id
                
                # Actualizar el documento agregando call_duration_secs_audio en la raíz
                update_data = {
                    'call_duration_secs_audio': round(duration_seconds, 2)  # Redondear a 2 decimales
                }
                
                doc.reference.update(update_data)
                
                logger.info(f"✅ Duración de audio actualizada para {conversation_id}")
                logger.info(f"   📊 Document ID: {doc_id}")
                logger.info(f"   🕐 Duración: {duration_seconds:.2f} segundos")
                
                return True
            else:
                logger.warning(f"⚠️ No se encontró registro de llamada para actualizar duración: {conversation_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error actualizando duración de audio para {conversation_id}: {e}")
            return False