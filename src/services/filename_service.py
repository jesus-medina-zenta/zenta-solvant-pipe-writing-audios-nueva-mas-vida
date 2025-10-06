"""
Servicio especializado para construcción de nombres de archivos de audio.
"""
from datetime import datetime
from typing import Optional
import re

from src.models.firestore_records import DynamicVariables, FilenameComponents


from src.utils.logger import get_logger

logger = get_logger(__name__)


class FilenameService:
    """
    Servicio para construir nombres de archivos basados en registros de llamadas.
    """

    @staticmethod
    def build_filename_from_call_record(call_record: DynamicVariables) -> str:
        """
        Construye el nombre del archivo basado en el registro de llamada.
        """
        try:
            logger.info(f"🏗️ Iniciando construcción de filename para: {call_record.conversation_id}")
            
            # Debug: mostrar campos principales disponibles
            logger.debug(f"📊 Datos disponibles:")
            logger.debug(f"   - RUT: {call_record.rut}")
            logger.debug(f"   - DV: {call_record.dv}")
            logger.debug(f"   - system__called_number: {call_record.system__called_number}")
            logger.debug(f"   - system__caller_id: {call_record.system__caller_id}")
            logger.debug(f"   - event_timestamp: {call_record.event_timestamp}")
            
            components = FilenameService._extract_filename_components(call_record)
            filename = components.build_filename()
            
            logger.info(f"✅ Nombre construido exitosamente: {filename}")
            logger.info(f"   📊 Componentes usados:")
            logger.info(f"      - Código: {components.codigo_finalizacion}")
            logger.info(f"      - Fecha/Hora: {components.fecha_hora}")
            logger.info(f"      - RUT: {components.rut_cliente}")
            logger.info(f"      - Fecha corta: {components.fecha_corta}")
            logger.info(f"      - Teléfono: {components.telefono}")
            
            return filename
            
        except Exception as e:
            logger.error(f"❌ Error construyendo nombre de archivo: {e}")
            logger.error(f"   📋 Conversation ID: {getattr(call_record, 'conversation_id', 'N/A')}")
            
            # Nombre de respaldo más informativo
            conv_id = getattr(call_record, 'conversation_id', 'unknown')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            fallback_name = f"PP_error_{timestamp}_{conv_id[-10:]}_fallback.wav"
            logger.warning(f"⚠️ Usando nombre de respaldo: {fallback_name}")
            return fallback_name
    
    @staticmethod
    def _extract_filename_components(call_record: DynamicVariables) -> FilenameComponents:
        """
        Extrae los componentes del nombre del archivo desde el registro de llamada.
        """
        # Código de finalización desde analysis.data_collection_results.final_call_outcome.value
        codigo_finalizacion = FilenameService._extract_codigo_finalizacion(call_record)
        
        # Fecha y hora desde system__time_utc
        fecha_hora = FilenameService._extract_fecha_hora(call_record)
        
        # RUT del cliente desde system__caller_id
        rut_cliente = FilenameService._extract_rut_cliente(call_record)
        
        # Fecha corta (DDMMYY)
        fecha_corta = FilenameService._extract_fecha_corta(call_record)
        
        # Teléfono desde system__called_number
        telefono = FilenameService._extract_telefono(call_record)
        
        return FilenameComponents(
            codigo_finalizacion=codigo_finalizacion,
            fecha_hora=fecha_hora,
            rut_cliente=rut_cliente,
            fecha_corta=fecha_corta,
            telefono=telefono,
            extension="wav"
        )
    
    @staticmethod
    def _extract_codigo_finalizacion(call_record: DynamicVariables) -> str:
        """
        Extrae el código de finalización desde analysis.data_collection_results.final_call_outcome.value.
        
        Args:
            call_record: Registro de llamada con los datos de analysis
            
        Returns:
            str: Código de finalización extraído de la estructura
        """
        # Navegar por la estructura: analysis.data_collection_results.final_call_outcome.value
        try:
            analysis = getattr(call_record, 'analysis', {})
            data_collection_results = analysis.get('data_collection_results', {})
            final_call_outcome = data_collection_results.get('final_call_outcome', {})
            value = final_call_outcome.get('value', '')

            # Convertir a string
            codigo = str(value).strip()

            if not codigo or codigo == '':
                logger.warning(f"Código de finalización vacío para {call_record.conversation_id}")

            logger.info(f"Código de finalización extraído: '{codigo}' para {call_record.conversation_id}")
            logger.info(f"📋 Código de finalización extraído: '{codigo}' para {call_record.conversation_id}")
            return codigo
        except Exception as e:
            logger.error(f"Error extrayendo código de finalización: {e}")
            return "unknown"
    
    @staticmethod
    def _extract_fecha_hora(call_record: DynamicVariables) -> str:
        """
        Extrae fecha y hora en formato YYYYMMDD-HHMMSS desde event_timestamp.
        """
        try:
            # Usar event_timestamp (Unix timestamp)
            if call_record.event_timestamp:
                dt = datetime.fromtimestamp(call_record.event_timestamp)
                return dt.strftime("%Y%m%d-%H%M%S")
            
            # Fallback: processed_at si está disponible
            elif call_record.processed_at:
                from dateutil import parser
                dt = parser.parse(call_record.processed_at)
                return dt.strftime("%Y%m%d-%H%M%S")
                
        except Exception as e:
            logger.warning(f"⚠️ Error parseando fecha: {e}")
        
        # Fecha de respaldo
        return datetime.now().strftime("%Y%m%d-%H%M%S")
    
    @staticmethod
    def _extract_rut_cliente(call_record: DynamicVariables) -> str:
        """
        Extrae RUT del cliente (solo 8 dígitos, sin DV).
        """
        try:
            # Verificar si tenemos RUT
            if call_record.rut:
                rut_num = str(call_record.rut).strip()
                # Extraer solo dígitos
                rut_digits = ''.join(filter(str.isdigit, rut_num))
                
                if len(rut_digits) >= 7:
                    # Asegurar exactamente 8 dígitos
                    if len(rut_digits) == 7:
                        rut_8_digits = rut_digits.zfill(8)  # Agregar 0 al inicio
                    else:
                        rut_8_digits = rut_digits[:8]  # Tomar primeros 8
                    
                    logger.info(f"📋 RUT extraído (8 dígitos): {rut_8_digits}")
                    return rut_8_digits
            
            # Buscar en otros campos posibles
            elif hasattr(call_record, 'system__caller_id') and call_record.system__caller_id:
                caller_id = str(call_record.system__caller_id)
                # Extraer números que puedan ser RUT
                numbers = ''.join(filter(str.isdigit, caller_id))
                if len(numbers) >= 7:
                    rut_8_digits = numbers[:8].zfill(8)
                    logger.warning(f"📋 RUT extraído de caller_id: {rut_8_digits}")
                    return rut_8_digits
            
        except Exception as e:
            logger.warning(f"⚠️ Error extrayendo RUT: {e}")
        
        return "00000000"  # RUT de respaldo (8 dígitos)
    
    @staticmethod
    def _extract_fecha_corta(call_record: DynamicVariables) -> str:
        """
        Extrae fecha corta en formato DDMMYY desde event_timestamp.
        """
        try:
            if call_record.event_timestamp:
                dt = datetime.fromtimestamp(call_record.event_timestamp)
                return dt.strftime("%d%m%y")
            elif call_record.processed_at:
                from dateutil import parser
                dt = parser.parse(call_record.processed_at)
                return dt.strftime("%d%m%y")
                
        except Exception as e:
            logger.warning(f"⚠️ Error extrayendo fecha_corta: {e}")
        
        return datetime.now().strftime("%d%m%y")
    
    @staticmethod
    def _extract_telefono(call_record: DynamicVariables) -> str:
        """
        Extrae número de teléfono (exactamente 10 dígitos).
        """
        try:
            # Opción 1: system__called_number (número al que se llamó - preferido)
            if call_record.system__called_number:
                phone = str(call_record.system__called_number)
                cleaned_phone = phone.replace("+", "").replace("-", "").replace(" ", "")
                phone_digits = ''.join(filter(str.isdigit, cleaned_phone))
                
                logger.debug(f"📱 Procesando teléfono: {phone} -> {phone_digits}")
                
                # Para números chilenos que empiecen con 56
                if phone_digits.startswith('56'):
                    # Quitar el código 56 y agregar 0 al inicio
                    without_country_code = phone_digits[2:]  # Quitar 56
                    final_phone = '0' + without_country_code  # Agregar 0
                    
                    # Asegurar exactamente 10 dígitos
                    if len(final_phone) > 10:
                        final_phone = final_phone[:10]  # Truncar a 10 dígitos
                    elif len(final_phone) < 10:
                        final_phone = final_phone.ljust(10, '0')  # Completar con 0s
                    
                    logger.info(f"📱 Teléfono chileno extraído: {phone} -> {final_phone}")
                    return final_phone
                
                # Para números que ya están en formato correcto (10 dígitos)
                elif len(phone_digits) == 10:
                    logger.info(f"📱 Teléfono directo (10 dígitos): {phone_digits}")
                    return phone_digits
                
                # Para números de más de 10 dígitos, tomar los últimos 10
                elif len(phone_digits) > 10:
                    final_phone = phone_digits[-10:]
                    logger.info(f"📱 Teléfono truncado: {phone} -> {final_phone}")
                    return final_phone
            
            # Opción 2: system__caller_id (quien llamó)
            if call_record.system__caller_id:
                phone = str(call_record.system__caller_id)
                cleaned_phone = phone.replace("+", "").replace("-", "").replace(" ", "")
                phone_digits = ''.join(filter(str.isdigit, cleaned_phone))
                
                logger.debug(f"📱 Procesando caller_id: {phone} -> {phone_digits}")
                
                # Aplicar la misma lógica para caller_id
                if phone_digits.startswith('56'):
                    without_country_code = phone_digits[2:]
                    final_phone = '0' + without_country_code
                    
                    if len(final_phone) > 10:
                        final_phone = final_phone[:10]
                    elif len(final_phone) < 10:
                        final_phone = final_phone.ljust(10, '0')
                    
                    logger.info(f"📱 Teléfono chileno de caller_id: {phone} -> {final_phone}")
                    return final_phone
                
                elif len(phone_digits) == 10:
                    logger.info(f"📱 Caller_id directo (10 dígitos): {phone_digits}")
                    return phone_digits
                
                elif len(phone_digits) > 10:
                    final_phone = phone_digits[-10:]
                    logger.info(f"📱 Caller_id truncado: {phone} -> {final_phone}")
                    return final_phone
            
            # Opción 3: Buscar en dynamic_variables
            if hasattr(call_record, 'dynamic_variables') and call_record.dynamic_variables:
                dv = call_record.dynamic_variables
                
                for field_name in ['system__called_number', 'system__caller_id']:
                    if isinstance(dv, dict) and field_name in dv:
                        phone = str(dv[field_name])
                        cleaned_phone = phone.replace("+", "").replace("-", "").replace(" ", "")
                        phone_digits = ''.join(filter(str.isdigit, cleaned_phone))
                        
                        logger.debug(f"📱 Procesando {field_name} de dynamic_variables: {phone} -> {phone_digits}")
                        
                        # Aplicar la misma lógica
                        if phone_digits.startswith('56'):
                            without_country_code = phone_digits[2:]
                            final_phone = '0' + without_country_code
                            
                            if len(final_phone) > 10:
                                final_phone = final_phone[:10]
                            elif len(final_phone) < 10:
                                final_phone = final_phone.ljust(10, '0')
                            
                            logger.info(f"📱 Teléfono chileno de {field_name}: {phone} -> {final_phone}")
                            return final_phone
                        
                        elif len(phone_digits) == 10:
                            logger.info(f"📱 {field_name} directo (10 dígitos): {phone_digits}")
                            return phone_digits
                        
                        elif len(phone_digits) > 10:
                            final_phone = phone_digits[-10:]
                            logger.info(f"📱 {field_name} truncado: {phone} -> {final_phone}")
                            return final_phone
            
            # Opción 4: Generar desde RUT si no hay teléfono
            if call_record.rut:
                rut_digits = ''.join(filter(str.isdigit, str(call_record.rut)))
                if len(rut_digits) >= 7:
                    # Generar pseudo-teléfono: 09 + 8 dígitos del RUT
                    pseudo_phone = f"09{rut_digits[:8]}"
                    logger.warning(f"📱 Teléfono generado desde RUT: {pseudo_phone}")
                    return pseudo_phone
            
        except Exception as e:
            logger.warning(f"⚠️ Error extrayendo teléfono: {e}")
        
        # Teléfono de respaldo
        logger.warning("📱 Usando teléfono de respaldo: 0900000000")
        return "0900000000"
    
    
    @staticmethod
    def extract_conversation_id_from_filename(filename: str) -> Optional[str]:
        """
        Extrae el conversation_id del nombre del archivo.
        
        Args:
            filename: Nombre del archivo (ej: conv_2701k60k4615eb59e4tba16dgfw6.mp3)
            
        Returns:
            Conversation ID extraído o None si no se encuentra
        """
        try:
            # Patrón para extraer conversation_id
            pattern = r'(conv_[a-zA-Z0-9]+)'
            match = re.search(pattern, filename)
            
            if match:
                conv_id = match.group(1)
                return conv_id
            else:
                logger.warning(f"⚠️ No se pudo extraer conversation ID de: {filename}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo conversation ID de {filename}: {e}")
            return None