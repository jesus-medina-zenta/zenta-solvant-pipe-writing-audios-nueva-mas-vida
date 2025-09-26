"""
Modelos de datos usando Pydantic para validación.
"""
from datetime import datetime, timezone  # ✅ IMPORTAR TIMEZONE CORRECTAMENTE
from typing import Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


class DataRecord(BaseModel):
    """
    Modelo base para registros de datos.
    Personaliza este modelo según tus necesidades específicas.
    """
    
    id: int = Field(..., description="Identificador único del registro")
    name: str = Field(..., min_length=1, max_length=255, description="Nombre del registro")
    value: Optional[float] = Field(None, description="Valor numérico opcional")
    category: Optional[str] = Field(None, max_length=100, description="Categoría del registro")
    is_active: bool = Field(True, description="Indica si el registro está activo")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Fecha de creación")
    updated_at: Optional[datetime] = Field(None, description="Fecha de última actualización")


class ProcessingStats(BaseModel):
    """
    Modelo para estadísticas de procesamiento del pipeline de audio.
    """
    
    total_files: int = Field(0, description="Total de archivos")
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
        if self.total_files == 0:
            return 0.0
        return (self.uploaded_files / self.total_files) * 100
    
    @property 
    def completion_rate(self) -> float:
        """Calcula la tasa de completitud (procesados vs total)."""
        if self.total_files == 0:
            return 0.0
        processed = self.uploaded_files + self.failed_files
        return (processed / self.total_files) * 100
    
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


class ErrorRecord(BaseModel):
    """
    Modelo para registros de errores.
    """
    
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_type: str = Field(..., description="Tipo de error")
    error_message: str = Field(..., description="Mensaje de error")
    context: Optional[Dict[str, Any]] = Field(None, description="Contexto adicional del error")


class PipelineConfig(BaseModel):
    """
    Configuración del pipeline.
    """
    
    batch_size: int = Field(10, gt=0, description="Tamaño del lote")
    max_retries: int = Field(3, ge=0, description="Máximo reintentos")
    timeout_seconds: int = Field(300, gt=0, description="Timeout en segundos")
    enable_logging: bool = Field(True, description="Habilitar logging detallado")