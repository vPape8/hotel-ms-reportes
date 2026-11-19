import { kafkaConfig, topicoDlt } from '../config/kafka.config';
import { log } from '../logs/logger';
import { ContextoRegistro, KafkaListener } from '../messaging/KafkaListener';

/**
 * Lee el dead-letter topic y registra cada mensaje muerto con su motivo (como verMuertos / cobros-dlt en la guía 3.3.3).
 * El DLT lo comparten varios grupos: este listener solo registra los que mandó el grupo "reportes".
 */
export class DltListener {
	@KafkaListener({
		topics: [topicoDlt(kafkaConfig.topicos.reservas)],
		groupId: kafkaConfig.grupos.reportesDlt,
		enviarADlt: false
	})
	async verMuertos(cuerpo: unknown, contexto: ContextoRegistro): Promise<void> {
		if (contexto.headers['kafka_original-consumer-group'] !== kafkaConfig.grupos.reportes) {
			return;
		}
		const evento = (typeof cuerpo === 'object' && cuerpo !== null ? cuerpo : {}) as { eventoId?: unknown; tipo?: unknown };
		log('error', '[DLT]', {
			key: contexto.key,
			particion: contexto.particion,
			offset: contexto.offset,
			eventoId: evento.eventoId,
			tipo: evento.tipo,
			topicOriginal: contexto.headers['kafka_original-topic'],
			offsetOriginal: contexto.headers['kafka_original-offset'],
			excepcion: contexto.headers['kafka_exception-fqcn'],
			motivo: contexto.headers['kafka_exception-message'],
			intentos: contexto.headers.kafka_intentos
		});
	}
}
