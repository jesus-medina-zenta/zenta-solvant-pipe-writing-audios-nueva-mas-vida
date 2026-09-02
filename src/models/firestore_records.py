"""
Modelos de datos usando Pydantic para validación del pipeline de audio.
"""
from datetime import datetime, timezone
from typing import Optional, Any, Dict, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from enum import Enum


class AudioStatus(str, Enum):
    """Estados posibles de procesamiento de audio."""
    AUDIO_SAVED_SUCCESSFULLY = "AUDIO_SAVED_SUCCESSFULLY"
    AUDIO_SAVED_IN_BUCKET = "AUDIO_SAVED_IN_BUCKET"
    PROCESSING = "PROCESSING"
    CONVERTED_TO_WAV = "CONVERTED_TO_WAV" 
    UPLOADED_TO_SFTP = "UPLOADED_TO_SFTP"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"


class AudioStatusRecord(BaseModel):
    """
    Modelo para registros de audios_status en Firestore.
    """
    
    id_conversacion: str = Field(..., description="ID de conversación")
    status: AudioStatus = Field(..., description="Estado del audio")
    time_stamp: int = Field(..., description="Timestamp Unix de creación")
    update_at: int = Field(..., description="Timestamp Unix de última actualización")
    
    @field_validator('id_conversacion')
    @classmethod
    def validate_conversation_id(cls, v):
        """Valida que el ID de conversación tenga el formato correcto."""
        if not v.startswith('conv_'):
            raise ValueError('ID de conversación debe comenzar con "conv_"')
        if len(v) < 10:
            raise ValueError('ID de conversación muy corto')
        return v
    
    @property
    def created_datetime(self) -> datetime:
        """Convierte timestamp a datetime."""
        return datetime.fromtimestamp(self.time_stamp, tz=timezone.utc)
    
    @property
    def updated_datetime(self) -> datetime:
        """Convierte update_at a datetime."""
        return datetime.fromtimestamp(self.update_at, tz=timezone.utc)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id_conversacion": "conv_2701k60k4615eb59e4tba16dgfw6",
                "status": "AUDIO_SAVED_IN_BUCKET",
                "time_stamp": 1758809219,
                "update_at": 1758809219
            }
        }
    )

class DynamicVariables(BaseModel):
    """
    Modelo para variables dinámicas del registro de llamada de Firestore.
    Basado en la estructura real de registros_llamadas.
    """
    
    # Campos principales del documento
    conversation_id: Optional[str] = Field(None, description="ID de conversación")
    agent_id: Optional[str] = Field(None, description="ID del agente")
    call_charge: Optional[int] = Field(None, description="Costo de la llamada")
    call_duration_secs: Optional[int] = Field(None, description="Duración de la llamada en segundos")
    event_timestamp: Optional[int] = Field(None, description="Timestamp del evento")
    llm_charge: Optional[int] = Field(None, description="Costo de LLM")
    llm_price: Optional[int] = Field(None, description="Precio de LLM")
    llm_usage: Optional[Dict[str, Any]] = Field(None, description="Uso de LLM")
    main_language: Optional[str] = Field(None, description="Idioma principal")
    processed_at: Optional[str] = Field(None, description="Timestamp de procesamiento")
    status: Optional[str] = Field(None, description="Estado del registro")
    termination_reason: Optional[str] = Field(None, description="Razón de terminación")
    
    # Campos del mapa analysis
    analysis: Optional[Dict[str, Any]] = Field(None, description="Datos de análisis")
    
    # Campos del mapa multivoice
    multivoice: Optional[Dict[str, Any]] = Field(None, description="Datos multivoice")
    
    # Campos del mapa dynamic_variables (los que necesitamos para el filename)
    agent_name: Optional[str] = Field(None, description="Nombre del agente")
    days_overdue: Optional[str] = Field(None, description="Días de mora")
    down_payment_option1: Optional[str] = Field(None, description="Opción de pago inicial")
    due_date: Optional[str] = Field(None, description="Fecha de vencimiento")
    dv: Optional[str] = Field(None, description="Dígito verificador")
    mail: Optional[str] = Field(None, description="Email del cliente")
    monthly_payment: Optional[str] = Field(None, description="Pago mensual")
    name: Optional[str] = Field(None, description="Nombre del cliente")
    paternal_surname: Optional[str] = Field(None, description="Apellido paterno")
    rut: Optional[str] = Field(None, description="RUT del cliente")
    system__agent_id: Optional[str] = Field(None, description="ID del agente del sistema")
    system__call_duration_secs: Optional[int] = Field(None, description="Duración de la llamada")
    system__called_number: Optional[str] = Field(None, description="Número al que se llamó")
    system__caller_id: Optional[str] = Field(None, description="Número desde el que llamó el cliente")
    
    # Configuración para permitir campos adicionales
    model_config = ConfigDict(
        extra='allow',  # Permite campos adicionales
        str_strip_whitespace=True,
        validate_assignment=True
    )

    @model_validator(mode='before')
    @classmethod
    def _fill_rut_from_dynamic_variables(cls, data: Any) -> Any:
        """
        El RUT y el teléfono del cliente no están en la raíz del documento de
        registros_llamadas, sino dentro de dynamic_variables (rut_deudor,
        system__called_number / system__caller_id), que son las variables
        dinámicas reales del agente actual.
        """
        if not isinstance(data, dict):
            return data
        dynamic_variables = data.get('dynamic_variables') or {}
        if not isinstance(dynamic_variables, dict):
            return data
        if not data.get('rut') and dynamic_variables.get('rut_deudor'):
            data['rut'] = dynamic_variables['rut_deudor']
        if not data.get('system__called_number') and dynamic_variables.get('system__called_number'):
            data['system__called_number'] = dynamic_variables['system__called_number']
        if not data.get('system__caller_id') and dynamic_variables.get('system__caller_id'):
            data['system__caller_id'] = dynamic_variables['system__caller_id']
        return data

    def get_client_phone(self) -> Optional[str]:
        """
        En este caso, no hay campo de teléfono explícito en dynamic_variables.
        Podríamos extraerlo del conversation_id o usar un valor por defecto.
        """
        return None
    
    def get_full_rut(self) -> Optional[str]:
        """
        Combina RUT + DV para formar el RUT completo.
        """
        if self.rut and self.dv:
            return f"{self.rut}-{self.dv}"
        return self.rut



class AudioProcessingTask(BaseModel):
    """
    Modelo para una tarea de procesamiento de audio.
    """
    
    conversation_id: str = Field(..., description="ID de conversación")
    audio_status: AudioStatusRecord = Field(..., description="Estado del audio")
    user_variables: Optional[DynamicVariables] = Field(None, description="Variables del usuario")
    original_filename: Optional[str] = Field(None, description="Nombre original del archivo")
    target_filename: Optional[str] = Field(None, description="Nombre objetivo del archivo")
    local_path: Optional[str] = Field(None, description="Ruta local temporal")
    converted_path: Optional[str] = Field(None, description="Ruta del archivo convertido")
    processing_status: Literal["pending", "downloading", "converting", "uploading", "completed", "failed"] = Field(default="pending")
    error_message: Optional[str] = Field(None, description="Mensaje de error si falló")
    
    @field_validator('conversation_id')
    @classmethod
    def validate_conversation_id(cls, v):
        """Valida que el ID de conversación tenga el formato correcto."""
        if not v.startswith('conv_'):
            raise ValueError('ID de conversación debe comenzar con "conv_"')
        return v


class FilenameComponents(BaseModel):
    """
    Componentes para construir nombres de archivos de audio.
    Estructura: PP_YYYYMMDD-HHMMSS_RUTCLIENTE_DDMMYY_TELEFONO.wav
    """
    
    codigo_finalizacion: str = Field(..., description="Código de finalización (ej: PP)")
    fecha_hora: str = Field(..., description="Fecha y hora en formato YYYYMMDD-HHMMSS")
    rut_cliente: str = Field(..., description="RUT del cliente (8 dígitos sin DV)")
    fecha_corta: str = Field(..., description="Fecha corta en formato DDMMYY")
    telefono: str = Field(..., description="Número de teléfono (10 dígitos)")
    extension: str = Field(default="wav", description="Extensión del archivo")
    
    @field_validator('rut_cliente')
    def validate_rut_cliente(cls, v):
        """Validar que el RUT tenga exactamente 8 dígitos"""
        cleaned_rut = str(v).replace("-", "").replace(".", "").replace(" ", "")
        # Extraer solo los dígitos (sin DV)
        rut_digits = ''.join(filter(str.isdigit, cleaned_rut))
        
        if len(rut_digits) < 7:
            # Completar con ceros a la izquierda si es muy corto
            rut_digits = rut_digits.zfill(8)
        elif len(rut_digits) > 8:
            # Tomar los primeros 8 dígitos si es muy largo
            rut_digits = rut_digits[:8]
        elif len(rut_digits) == 7:
            # Completar con un cero a la izquierda
            rut_digits = rut_digits.zfill(8)
        
        return rut_digits
    
    @field_validator('telefono')
    def validate_telefono(cls, v):
        """Validar que el teléfono tenga exactamente 10 dígitos"""
        cleaned_phone = str(v).replace("+", "").replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
        phone_digits = ''.join(filter(str.isdigit, cleaned_phone))
        
        if len(phone_digits) < 10:
            # Completar con ceros a la izquierda si es muy corto
            phone_digits = phone_digits.zfill(10)
        elif len(phone_digits) > 10:
            # Para números chilenos que empiecen con 56, quitar el prefijo
            if phone_digits.startswith('56') and len(phone_digits) == 12:
                phone_digits = phone_digits[2:]  # Quitar 56
            else:
                # Tomar los últimos 10 dígitos
                phone_digits = phone_digits[-10:]
        
        return phone_digits
    
    def build_filename(self) -> str:
        """
        Construye el nombre completo del archivo.
        Formato: PP_YYYYMMDD-HHMMSS_RUTCLIENTE_DDMMYY_TELEFONO.wav
        """
        return f"{self.codigo_finalizacion}_{self.fecha_hora}_{self.rut_cliente}_{self.fecha_corta}_{self.telefono}.{self.extension}"
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "codigo_finalizacion": "PP",
                "fecha_hora": "20230123-091701", 
                "rut_cliente": "06394791",
                "fecha_corta": "230913",
                "telefono": "0992535244",
                "extension": "wav"
            }
        }
    )


class ProcessingStats(BaseModel):
    """
    Modelo para estadísticas de procesamiento del pipeline de audio.
    """
    
    total_audio_files: int = Field(0, description="Total de archivos de audio")
    pending_files: int = Field(0, description="Archivos pendientes")
    downloaded_files: int = Field(0, description="Archivos descargados")
    converted_files: int = Field(0, description="Archivos convertidos")
    uploaded_files: int = Field(0, description="Archivos subidos")
    failed_files: int = Field(0, description="Archivos que fallaron")
    updated_status_files: int = Field(0, description="Estados actualizados en Firestore")
    processing_time_seconds: float = Field(0.0, description="Tiempo total de procesamiento")
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = Field(None)
    
    @property
    def success_rate(self) -> float:
        """Calcula la tasa de éxito del procesamiento."""
        if self.total_audio_files == 0:
            return 0.0
        return (self.uploaded_files / self.total_audio_files) * 100
    
    @property
    def completion_rate(self) -> float:
        """Calcula la tasa de completitud (procesados vs total)."""
        if self.total_audio_files == 0:
            return 0.0
        processed = self.uploaded_files + self.failed_files
        return (processed / self.total_audio_files) * 100
    
    def add_downloaded(self) -> None:
        """Incrementa contador de archivos descargados."""
        self.downloaded_files += 1
    
    def add_converted(self) -> None:
        """Incrementa contador de archivos convertidos."""
        self.converted_files += 1
    
    def add_uploaded(self) -> None:
        """Incrementa contador de archivos subidos."""
        self.uploaded_files += 1
    
    def add_failed(self) -> None:
        """Incrementa contador de archivos fallidos."""
        self.failed_files += 1
    
    def add_status_updated(self) -> None:
        """Incrementa contador de estados actualizados."""
        self.updated_status_files += 1
    
    def finish(self) -> None:
        """Marca el procesamiento como finalizado."""
        self.end_time = datetime.now(timezone.utc)
        if self.start_time:
            self.processing_time_seconds = (self.end_time - self.start_time).total_seconds()


class AudioProcessingError(BaseModel):
    """
    Modelo para errores durante el procesamiento de audio.
    """
    
    conversation_id: str = Field(..., description="ID de conversación")
    stage: Literal["query", "download", "convert", "upload", "status_update"] = Field(..., description="Etapa donde ocurrió el error")
    error_type: str = Field(..., description="Tipo de error")
    error_message: str = Field(..., description="Mensaje de error")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_data: Optional[Dict[str, Any]] = Field(None, description="Datos en bruto que causaron el error")
    recoverable: bool = Field(default=True, description="Si el error es recuperable")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "conversation_id": "conv_2701k60k4615eb59e4tba16dgfw6",
                "stage": "convert",
                "error_type": "ConversionError",
                "error_message": "Archivo de audio corrupto",
                "timestamp": "2023-01-01T12:00:00Z",
                "raw_data": {"file_size": 0, "format": "mp3"},
                "recoverable": False
            }
        }
    )


class PipelineConfig(BaseModel):
    """
    Configuración específica del pipeline de audio.
    """
    
    batch_size: int = Field(10, gt=0, description="Tamaño del lote para procesamiento")
    max_retries: int = Field(3, ge=0, description="Máximo número de reintentos")
    conversion_timeout: int = Field(300, gt=0, description="Timeout para conversión en segundos")
    enable_status_updates: bool = Field(True, description="Habilitar actualizaciones de estado")
    delete_after_upload: bool = Field(True, description="Borrar archivos del bucket después de subir")
    max_concurrent_downloads: int = Field(5, gt=0, description="Máximo descargas concurrentes")
    max_concurrent_conversions: int = Field(3, gt=0, description="Máximo conversiones concurrentes")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "batch_size": 10,
                "max_retries": 3,
                "conversion_timeout": 300,
                "enable_status_updates": True,
                "delete_after_upload": True,
                "max_concurrent_downloads": 5,
                "max_concurrent_conversions": 3
            }
        }
    )