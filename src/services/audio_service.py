"""
Servicio para analizar propiedades de archivos de audio.
"""
import os
import shutil
from typing import Optional, Dict, Any
from pydub import AudioSegment
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _resolve_ffmpeg_binaries() -> None:
    """
    Asegura que pydub encuentre ffmpeg/ffprobe aunque el PATH del proceso
    no se haya refrescado (típico en Windows justo después de instalar
    con winget, sin reiniciar el host de la terminal).

    pydub.utils.get_prober_name() ignora AudioSegment.ffprobe y siempre
    busca el binario vía os.environ["PATH"], así que además de fijar los
    atributos de AudioSegment hay que anteponer el directorio al PATH
    del proceso actual.
    """
    known_windows_dirs = [
        r"C:\ffmpeg\bin",
        os.path.expandvars(
            r"%LOCALAPPDATA%\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe"
            r"\ffmpeg-9.0.1-full_build\bin"
        ),
    ]

    ffmpeg_path = shutil.which("ffmpeg")
    ffprobe_path = shutil.which("ffprobe")
    found_dir = None

    if not ffmpeg_path or not ffprobe_path:
        for candidate_dir in known_windows_dirs:
            candidate_ffmpeg = os.path.join(candidate_dir, "ffmpeg.exe")
            candidate_ffprobe = os.path.join(candidate_dir, "ffprobe.exe")
            if os.path.exists(candidate_ffmpeg) and os.path.exists(candidate_ffprobe):
                ffmpeg_path = ffmpeg_path or candidate_ffmpeg
                ffprobe_path = ffprobe_path or candidate_ffprobe
                found_dir = candidate_dir
                break

    if found_dir and found_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = found_dir + os.pathsep + os.environ.get("PATH", "")

    if ffmpeg_path:
        AudioSegment.converter = ffmpeg_path
    if ffprobe_path:
        AudioSegment.ffprobe = ffprobe_path

    if not ffmpeg_path or not ffprobe_path:
        logger.warning("⚠️ No se pudo ubicar ffmpeg/ffprobe automáticamente")


_resolve_ffmpeg_binaries()


class AudioAnalyzerService:
    """
    Servicio para analizar archivos de audio y extraer metadatos.
    """
    
    @staticmethod
    def get_audio_duration(file_path: str, format_hint: Optional[str] = None) -> Optional[float]:
        """
        Obtiene la duración del audio en segundos.
        
        Args:
            file_path: Ruta al archivo de audio
            format_hint: Formato del archivo (mp3, wav, etc.)
            
        Returns:
            Duración en segundos (float) o None si hay error
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"❌ Archivo no existe: {file_path}")
                return None
            
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.error(f"❌ Archivo vacío: {file_path}")
                return None
            
            logger.debug(f"🎵 Analizando duración de: {file_path}")
            
            # Detectar formato automáticamente si no se proporciona
            if not format_hint:
                format_hint = AudioAnalyzerService._detect_audio_format(file_path)
            
            # Cargar audio según el formato
            if format_hint == "mp3":
                audio = AudioSegment.from_mp3(file_path)
            elif format_hint == "wav":
                audio = AudioSegment.from_wav(file_path)
            elif format_hint == "m4a":
                audio = AudioSegment.from_file(file_path, format="m4a")
            elif format_hint == "flac":
                audio = AudioSegment.from_flac(file_path)
            else:
                # Dejar que pydub detecte automáticamente
                audio = AudioSegment.from_file(file_path)
            
            # Obtener duración en segundos
            duration_seconds = len(audio) / 1000.0
            
            logger.info(f"🕐 Duración calculada: {duration_seconds:.2f} segundos ({AudioAnalyzerService._format_duration(duration_seconds)})")
            return duration_seconds
            
        except Exception as e:
            logger.error(f"❌ Error calculando duración de {file_path}: {e}")
            return None
    
    @staticmethod
    def get_audio_metadata(file_path: str, format_hint: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtiene metadatos completos del archivo de audio.
        """
        try:
            metadata = {
                "file_path": file_path,
                "file_exists": os.path.exists(file_path),
                "file_size_bytes": 0,
                "duration_seconds": None,
                "duration_formatted": None,
                "sample_rate": None,
                "channels": None,
                "format": format_hint or AudioAnalyzerService._detect_audio_format(file_path),
                "error": None
            }
            
            if not metadata["file_exists"]:
                metadata["error"] = "File not found"
                return metadata
            
            metadata["file_size_bytes"] = os.path.getsize(file_path)
            
            if metadata["file_size_bytes"] == 0:
                metadata["error"] = "Empty file"
                return metadata
            
            # Obtener duración
            duration = AudioAnalyzerService.get_audio_duration(file_path, format_hint)
            if duration is not None:
                metadata["duration_seconds"] = duration
                metadata["duration_formatted"] = AudioAnalyzerService._format_duration(duration)
                
                # Obtener propiedades adicionales del audio
                try:
                    if format_hint == "mp3":
                        audio = AudioSegment.from_mp3(file_path)
                    elif format_hint == "wav":
                        audio = AudioSegment.from_wav(file_path)
                    else:
                        audio = AudioSegment.from_file(file_path)
                    
                    metadata["sample_rate"] = audio.frame_rate
                    metadata["channels"] = audio.channels
                    
                except Exception as audio_error:
                    logger.warning(f"⚠️ Error obteniendo propiedades adicionales: {audio_error}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo metadatos de {file_path}: {e}")
            return {
                "file_path": file_path,
                "error": str(e),
                "duration_seconds": None
            }
    
    @staticmethod
    def _detect_audio_format(file_path: str) -> str:
        """Detecta el formato del archivo por su extensión."""
        try:
            extension = os.path.splitext(file_path)[1].lower()
            format_map = {
                ".mp3": "mp3",
                ".wav": "wav", 
                ".m4a": "m4a",
                ".flac": "flac",
                ".aac": "aac",
                ".ogg": "ogg"
            }
            return format_map.get(extension, "unknown")
        except Exception:
            return "unknown"
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Formatea la duración en formato MM:SS o HH:MM:SS."""
        try:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            
            if hours > 0:
                return f"{hours:02d}:{minutes:02d}:{secs:02d}"
            else:
                return f"{minutes:02d}:{secs:02d}"
        except Exception:
            return "00:00"