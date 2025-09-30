
from enum import Enum


class AudioStatus(str, Enum):
    """Estados posibles de procesamiento de audio."""
    AUDIO_SAVED_IN_BUCKET = "AUDIO_SAVED_IN_BUCKET"
    PROCESSING = "PROCESSING"
    CONVERTED_TO_WAV = "CONVERTED_TO_WAV" 
    UPLOADED_TO_SFTP = "UPLOADED_TO_SFTP"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"