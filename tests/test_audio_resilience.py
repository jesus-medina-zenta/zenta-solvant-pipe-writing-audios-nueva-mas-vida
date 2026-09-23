"""
Tests del manejo de audios vacíos, fallas transitorias de ffmpeg y reintentos.

Usan ffmpeg real sobre dos fixtures:
- empty_call.mp3: MP3 de 45 bytes (solo etiqueta ID3) como el que deja una llamada
  que se cortó al iniciar.
- valid_call.mp3: tono de 5 segundos.
"""
import asyncio
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ.setdefault("SFTP_HOST", "sftp.test")
os.environ.setdefault("SFTP_PORT", "22")
os.environ.setdefault("SFTP_USERNAME", "user")
os.environ.setdefault("SFTP_PASSWORD", "secret")
os.environ.setdefault("SFTP_UPLOAD_PATH", "/zentagrp/Audios")
os.environ.setdefault("FIRESTORE_PROJECT_ID", "test")
os.environ.setdefault("FIRESTORE_DATABASE", "test")
os.environ.setdefault("FIRESTORE_REGISTROS_LLAMADAS_COLLECTION", "registros_llamadas")
os.environ.setdefault("FIRESTORE_AUDIOS_STATUS_COLLECTION", "audios_status")
os.environ.setdefault("GCS_BUCKET_NAME", "bucket-test")

from src import pipeline as pipeline_module  # noqa: E402
from src.models.firestore_records import AudioStatus, AudioStatusRecord  # noqa: E402
from src.pipeline import Pipeline  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg no está instalado")


def _record(conversation_id: str) -> AudioStatusRecord:
    return AudioStatusRecord(
        id_conversacion=conversation_id,
        status=AudioStatus.AUDIO_SAVED_IN_BUCKET,
        time_stamp=1_700_000_000,
        update_at=1_700_000_000,
    )


def _build_pipeline(pending, fixture_by_id):
    """Pipeline con GCS, SFTP y Firestore simulados; ffmpeg y pydub son reales."""
    pipe = Pipeline()

    firestore = MagicMock()
    firestore._query_pending_audios_sync.return_value = pending
    firestore.requeue_stale_processing.return_value = 0
    firestore._update_audio_status_sync.return_value = True
    firestore.register_failed_attempt.return_value = AudioStatus.AUDIO_SAVED_IN_BUCKET
    firestore.update_call_duration_audio.return_value = True
    firestore._get_call_record_sync.return_value = MagicMock()
    pipe.firestore_service = firestore

    async def find_file(conversation_id):
        return {"name": f"audios/{conversation_id}.mp3"}

    async def download(blob_name, local_path):
        conversation_id = Path(blob_name).stem
        shutil.copy(FIXTURES / fixture_by_id[conversation_id], local_path)
        return True

    pipe._find_audio_file_in_bucket = find_file
    pipe.gcs_service = MagicMock()
    pipe.gcs_service.download_file = download
    pipe.gcs_service.disconnect = AsyncMock()

    pipe.sftp_service = MagicMock()
    pipe.sftp_service.load = AsyncMock(return_value=True)
    pipe.sftp_service.disconnect = AsyncMock()
    return pipe


def _statuses(firestore):
    """Estados escritos con _update_audio_status_sync, por conversación."""
    result = {}
    for call in firestore._update_audio_status_sync.call_args_list:
        result.setdefault(call.args[0], []).append(call.args[1])
    return result


@pytest.fixture(autouse=True)
def fast_retries():
    """Evita las esperas entre reintentos de conversión."""
    real_sleep = asyncio.sleep

    async def no_wait(delay, *args, **kwargs):
        return await real_sleep(0)

    with patch.object(pipeline_module.asyncio, "sleep", no_wait), \
            patch.object(pipeline_module.FilenameService, "build_filename_from_call_record",
                         side_effect=lambda record: "PP_test.wav"):
        yield


class TestNoAudioReason:
    def test_archivo_de_45_bytes_es_no_audio(self):
        assert Pipeline._no_audio_reason({"file_size_bytes": 45, "duration_seconds": None}) == "archivo de 45 bytes"

    def test_duracion_desconocida_en_archivo_grande_no_es_no_audio(self):
        # ffmpeg pudo fallar de forma transitoria: se debe intentar convertir igual
        assert Pipeline._no_audio_reason({"file_size_bytes": 1_532_205, "duration_seconds": None}) is None

    def test_duracion_menor_a_un_segundo_es_no_audio(self):
        assert Pipeline._no_audio_reason({"file_size_bytes": 5_000, "duration_seconds": 0.4}) == "duración de 0.40s"

    def test_audio_normal_es_valido(self):
        assert Pipeline._no_audio_reason({"file_size_bytes": 1_532_205, "duration_seconds": 95.76}) is None


class TestPipelineRun:
    def test_llamada_vacia_queda_no_audio_y_no_se_sube(self):
        pipe = _build_pipeline([_record("conv_empty0000001")], {"conv_empty0000001": "empty_call.mp3"})

        assert asyncio.run(pipe.run()) is True

        assert _statuses(pipe.firestore_service)["conv_empty0000001"][-1] == AudioStatus.NO_AUDIO
        pipe.firestore_service.register_failed_attempt.assert_not_called()
        pipe.sftp_service.load.assert_not_called()

    def test_audio_valido_se_sube_a_carpeta_ddmmyyyy_de_chile(self):
        pipe = _build_pipeline([_record("conv_valid0000001")], {"conv_valid0000001": "valid_call.mp3"})

        assert asyncio.run(pipe.run()) is True

        remote_path = pipe.sftp_service.load.call_args.args[0][0]["remote_path"]
        expected_folder = datetime.now(pipeline_module.SFTP_FOLDER_TZ).strftime("%d%m%Y")
        assert remote_path == f"/zentagrp/Audios/{expected_folder}/PP_test.wav"
        assert re.fullmatch(r"\d{8}", expected_folder)
        assert _statuses(pipe.firestore_service)["conv_valid0000001"][-1] == AudioStatus.UPLOADED_TO_SFTP

    def test_falla_transitoria_de_ffmpeg_se_reintenta_y_sube(self):
        pipe = _build_pipeline([_record("conv_flaky0000001")], {"conv_flaky0000001": "valid_call.mp3"})
        real_from_mp3 = pipeline_module.AudioSegment.from_mp3
        calls = {"n": 0}

        def flaky_from_mp3(path, *args, **kwargs):
            # El análisis de metadatos decodifica 2 veces; la conversión es la 3ª llamada
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("Decoding failed. ffmpeg returned error code: -6")
            return real_from_mp3(path, *args, **kwargs)

        with patch.object(pipeline_module.AudioSegment, "from_mp3", side_effect=flaky_from_mp3):
            assert asyncio.run(pipe.run()) is True

        assert _statuses(pipe.firestore_service)["conv_flaky0000001"][-1] == AudioStatus.UPLOADED_TO_SFTP
        pipe.firestore_service.register_failed_attempt.assert_not_called()

    def test_fallo_persistente_se_reencola_sin_fallar_el_job(self):
        pending = [_record("conv_valid0000001"), _record("conv_broken000001")]
        pipe = _build_pipeline(pending, {"conv_valid0000001": "valid_call.mp3",
                                         "conv_broken000001": "valid_call.mp3"})
        original_convert = pipe._convert_to_wav

        async def convert(input_path, output_path, extension):
            if "conv_broken000001" in input_path:
                return False
            return await original_convert(input_path, output_path, extension)

        pipe._convert_to_wav = convert

        # Un audio malo entre varios buenos no debe hacer que Cloud Run relance el job
        assert asyncio.run(pipe.run()) is True

        failed = pipe.firestore_service.register_failed_attempt.call_args
        assert failed.args[0] == "conv_broken000001"
        assert failed.args[2] == pipeline_module.MAX_ATTEMPTS
        assert pipe.sftp_service.load.call_count == 1

    def test_si_todo_falla_el_job_reporta_error(self):
        pipe = _build_pipeline([_record("conv_valid0000001")], {"conv_valid0000001": "valid_call.mp3"})
        pipe.sftp_service.load = AsyncMock(return_value=False)

        assert asyncio.run(pipe.run()) is False
        pipe.firestore_service.register_failed_attempt.assert_called_once()

    def test_reencola_audios_colgados_al_iniciar(self):
        pipe = _build_pipeline([], {})

        assert asyncio.run(pipe.run()) is True
        pipe.firestore_service.requeue_stale_processing.assert_called_once_with(
            pipeline_module.STALE_PROCESSING_SECONDS)


class TestRegisterFailedAttempt:
    def _service_with_doc(self, doc_data):
        from src.services.firestore_service import FirestoreService

        service = FirestoreService()
        service.is_connected = True
        doc_ref = MagicMock()
        doc_ref.get.return_value.to_dict.return_value = doc_data
        service.client = MagicMock()
        service.client.collection.return_value.document.return_value = doc_ref
        return service, doc_ref

    def test_primer_fallo_vuelve_a_la_cola(self):
        service, doc_ref = self._service_with_doc({"status": "PROCESSING"})

        assert service.register_failed_attempt("conv_x000000001", "boom", 3, "conv_x000000001") \
            == AudioStatus.AUDIO_SAVED_IN_BUCKET
        update = doc_ref.update.call_args.args[0]
        assert update["intentos"] == 1
        assert update["status"] == "AUDIO_SAVED_IN_BUCKET"

    def test_tercer_fallo_queda_failed(self):
        service, doc_ref = self._service_with_doc({"status": "PROCESSING", "intentos": 2})

        assert service.register_failed_attempt("conv_x000000001", "boom", 3, "conv_x000000001") \
            == AudioStatus.FAILED
        assert doc_ref.update.call_args.args[0]["status"] == "FAILED"
