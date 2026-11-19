import { log } from '../logs/logger';
import { ErrorNoRecuperable } from './errores';
import { ContextoRegistro, ListenerKafkaRegistrado } from './KafkaListener';

export interface RegistroKafka {
	topico: string;
	particion: number;
	offset: string;
	key: string | null;
	valor: Buffer | null;
	headers: Record<string, string>;
}

export interface MotivoDlt {
	excepcion: string;
	mensaje: string;
	intentos: number;
}

/** Lo que el procesamiento necesita de afuera (en los tests se reemplaza por dobles). */
export interface OperacionesRegistro {
	enviarADlt(registro: RegistroKafka, listener: ListenerKafkaRegistrado, motivo: MotivoDlt): Promise<void>;
	esperar(ms: number): Promise<void>;
}

export interface PoliticaReintentos {
	/** Intentos totales: el primero + los reintentos. 3 = FixedBackOff(1000, 2) de la guía 3.3.3. */
	intentos: number;
	esperaMs: number;
}

export type Resultado = 'OK' | 'DLT' | 'ERROR_EN_DLT';

/**
 * Procesa un mensaje como el DefaultErrorHandler del demo:
 *  - JSON roto o DTO inválido   -> [VALIDA] y directo al DLT, sin reintentos
 *  - ErrorNoRecuperable          -> directo al DLT
 *  - otro error (temporal)       -> [RETRY] hasta completar los intentos, esperando entre cada uno, y luego DLT
 * Cuando el mensaje termina en el DLT, la función vuelve normal: el offset se confirma y la partición
 * sigue con los siguientes mensajes (no queda bloqueada). Si mandar al DLT falla, lanza el error para que
 * Kafka no confirme el offset y el mensaje se vuelva a entregar (no se pierde).
 */
export async function procesarRegistro(
	registro: RegistroKafka,
	listener: ListenerKafkaRegistrado,
	miembro: number,
	politica: PoliticaReintentos,
	operaciones: OperacionesRegistro
): Promise<Resultado> {
	const contexto: ContextoRegistro = {
		topico: registro.topico,
		particion: registro.particion,
		offset: registro.offset,
		...(registro.key !== null ? { key: registro.key } : {}),
		headers: registro.headers,
		miembro
	};
	const datosLog = { grupo: listener.groupId, miembro, topic: registro.topico, particion: registro.particion, offset: registro.offset, key: registro.key };
	log('info', '[LEE]', datosLog);

	let datos: unknown;
	try {
		const texto = registro.valor?.toString('utf8') ?? '';
		let cuerpo: unknown;
		try {
			cuerpo = JSON.parse(texto);
		} catch {
			// En un listener de DLT se acepta cualquier contenido: justamente ahí llegan los mensajes rotos.
			if (!listener.enviarADlt) {
				cuerpo = texto;
			} else {
				throw new ErrorNoRecuperable('El valor no es JSON válido');
			}
		}
		datos = listener.validar(cuerpo);
	} catch (error) {
		const mensaje = error instanceof Error ? error.message : String(error);
		log('error', '[VALIDA]', { ...datosLog, motivo: mensaje, destino: listener.enviarADlt ? 'DLT' : 'descartado' });
		return mandarAlDlt(registro, listener, { excepcion: 'ErrorNoRecuperable', mensaje, intentos: 1 }, operaciones);
	}

	for (let intento = 1; intento <= politica.intentos; intento++) {
		try {
			await listener.manejar(datos, contexto);
			return 'OK';
		} catch (error) {
			const excepcion = error instanceof Error ? error.name : 'Error';
			const mensaje = error instanceof Error ? error.message : String(error);
			if (error instanceof ErrorNoRecuperable) {
				log('error', '[VALIDA]', { ...datosLog, motivo: mensaje, destino: 'DLT' });
				return mandarAlDlt(registro, listener, { excepcion, mensaje, intentos: intento }, operaciones);
			}
			if (!listener.enviarADlt) {
				log('error', 'ERROR_EN_LISTENER_DLT', { ...datosLog, motivo: mensaje });
				return 'ERROR_EN_DLT';
			}
			if (intento < politica.intentos) {
				log('warn', '[RETRY]', { ...datosLog, intento, de: politica.intentos, motivo: mensaje, esperaMs: politica.esperaMs });
				await operaciones.esperar(politica.esperaMs);
			} else {
				log('error', 'REINTENTOS_AGOTADOS', { ...datosLog, intentos: intento, motivo: mensaje });
				return mandarAlDlt(registro, listener, { excepcion, mensaje, intentos: intento }, operaciones);
			}
		}
	}
	return 'OK';
}

async function mandarAlDlt(
	registro: RegistroKafka,
	listener: ListenerKafkaRegistrado,
	motivo: MotivoDlt,
	operaciones: OperacionesRegistro
): Promise<Resultado> {
	if (!listener.enviarADlt) {
		return 'ERROR_EN_DLT';
	}
	await operaciones.enviarADlt(registro, listener, motivo);
	return 'DLT';
}
