import { kafkaConfig } from '../config/kafka.config';
import { log } from '../logs/logger';
import { ReservaEventoDto, validarEventoReserva } from '../messaging/eventoReserva';
import { ContextoRegistro, KafkaListener } from '../messaging/KafkaListener';
import { ReporteRepository } from '../repositories/reporteRepository';

/** Falla temporal de prueba: se reintenta y, al agotar los intentos, el evento termina en el DLT. */
export class FalloSimuladoError extends Error {
	constructor(idHabitacion: string) {
		super(`Fallo simulado: la base de reportes no responde (${idHabitacion})`);
		this.name = 'FalloSimuladoError';
	}
}

/**
 * Analítica en tiempo real: cada evento del tópico "reservas" actualiza la proyección de la que salen los KPIs.
 * Grupo "reportes" con concurrency 3: un miembro por partición (guía 3.3.1).
 */
export class ReportesListener {
	constructor(private readonly repositorio: ReporteRepository) {}

	@KafkaListener({
		topics: [kafkaConfig.topicos.reservas],
		groupId: kafkaConfig.grupos.reportes,
		concurrency: kafkaConfig.concurrencia,
		validar: validarEventoReserva
	})
	async actualizarKpis(evento: ReservaEventoDto, contexto: ContextoRegistro): Promise<void> {
		// Como el cliente "FALLA" del demo: esta habitación simula que la base de reportes está caída.
		if (evento.reserva.idHabitacion === kafkaConfig.habitacionFalla) {
			throw new FalloSimuladoError(evento.reserva.idHabitacion);
		}

		const resultado = await this.repositorio.aplicarEvento(evento);
		const datos = {
			eventoId: evento.eventoId,
			tipo: evento.tipo,
			codigo: evento.reserva.codigo,
			particion: contexto.particion,
			offset: contexto.offset,
			miembro: contexto.miembro
		};
		if (resultado === 'DUPLICADO') {
			log('warn', '[IDEMP]', { ...datos, detalle: 'evento ya aplicado, se ignora' });
			return;
		}
		log('info', '[REPORTE]', { ...datos, estado: evento.reserva.estado });
	}
}
