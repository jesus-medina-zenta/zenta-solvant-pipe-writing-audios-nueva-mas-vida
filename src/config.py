"""
Configuración de la aplicación usando Pydantic para validación.
"""
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class DatabaseConfig(BaseSettings):
    """Configuración para bases de datos PostgreSQL."""

    host: str = Field(default="localhost", description="Host de PostgreSQL")
    port: int = Field(default=5432, description="Puerto de PostgreSQL")
    database: str = Field(default="postgres", description="Nombre de la base de datos")
    user: str = Field(default="postgres", description="Usuario de PostgreSQL")
    password: str = Field(default="", description="Contraseña de PostgreSQL")

    model_config = {
        "env_prefix": "POSTGRES_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }

    @property
    def connection_string(self) -> str:
        """Retorna la cadena de conexión para PostgreSQL."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"



class AppConfig(BaseSettings):
    """Configuración principal de la aplicación."""

    # Configuración general
    app_name: str = Field(default="zenta-template-pipe-python", description="Nombre de la aplicación")
    log_level: str = Field(default="INFO", description="Nivel de logging")
    environment: str = Field(default="dev", description="Entorno de ejecución")

    # Configuración de procesamiento
    batch_size: int = Field(default=1000, description="Tamaño del lote para procesamiento")
    max_retries: int = Field(default=3, description="Número máximo de reintentos")
    retry_delay: int = Field(default=5, description="Delay entre reintentos en segundos")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"  # Ignora variables de entorno adicionales
    }

class SFTPConfig(BaseSettings):
    host: str
    port: int
    username: str
    password: str
    upload_path: str

    model_config = {
        "env_prefix": "SFTP_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }

class CloudStorageConfig(BaseSettings):
    """Configuración para Google Cloud Storage."""
    
    project_id: str = Field(default="", description="ID del proyecto de Google Cloud")
    bucket_name: str = Field(default="", description="Nombre del bucket")
    audio_prefix: str = Field(default="audio/", description="Prefijo para archivos de audio")
    
    model_config = {
        "env_prefix": "GCS_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


def get_sftp_config() -> SFTPConfig:
    return SFTPConfig()

def get_cloud_storage_config() -> CloudStorageConfig:
    """Obtiene la configuración de Cloud Storage."""
    return CloudStorageConfig()

def get_config() -> AppConfig:
    """
    Obtiene la configuración de la aplicación.
    Útil para testing y para evitar errores en importación.
    """
    return AppConfig()


def get_database_config() -> DatabaseConfig:
    """
    Obtiene la configuración de la base de datos.
    """
    return DatabaseConfig()


# Instancia global de configuración (solo se crea cuando se necesita)
config: Optional[AppConfig] = None


def init_config() -> AppConfig:
    """
    Inicializa la configuración global.
    """
    global config
    if config is None:
        config = AppConfig()
    return config
