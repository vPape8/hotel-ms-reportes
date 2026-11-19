// Configuración centralizada de Kafka (equivalente a spring.kafka en application.yml).
// Los tópicos los crea hotel-ms-reservas; aquí solo se leen sus nombres.
import { NOMBRE_SERVICIO } from './servicio';

function lista(variable: string | undefined): string[] {
	return (variable ?? '').split(',').map((valor) => valor.trim()).filter(Boolean);
}

function entero(variable: string | undefined, porDefecto: number): number {
	const valor = Number(variable);
	return Number.isInteger(valor) && valor > 0 ? valor : porDefecto;
}

export const kafkaConfig = {
	/** bootstrap-servers. Si está vacío no se conectan consumidores (tests y desarrollo sin Kafka). */
	brokers: lista(process.env.KAFKA_BROKERS),
	clientId: process.env.KAFKA_CLIENT_ID ?? NOMBRE_SERVICIO,
	reconexionMs: entero(process.env.KAFKA_RECONEXION_MS, 10000),

	consumidor: {
		/** Igual que auto-offset-reset: earliest. Solo aplica cuando el grupo todavía no tiene offset guardado. */
		fromBeginning: true,
		sessionTimeoutMs: 30000,
		heartbeatIntervalMs: 3000
	},

	/** FixedBackOff(1000, 2) de la guía 3.3.3: 3 intentos en total, 1 s entre cada uno. */
	reintentos: {
		intentos: entero(process.env.KAFKA_REINTENTOS_INTENTOS, 3),
		esperaMs: entero(process.env.KAFKA_REINTENTOS_ESPERA_MS, 1000)
	},

	/** El DLT de cada tópico se llama igual con ".DLT" (como pedidos.DLT). */
	sufijoDlt: '.DLT',

	topicos: {
		reservas: process.env.KAFKA_TOPICO_RESERVAS ?? 'reservas'
	},

	grupos: {
		/** Grupo que calcula los KPIs. Con concurrency 3, cada miembro lee una de las 3 particiones. */
		reportes: process.env.KAFKA_GRUPO ?? 'reportes',
		reportesDlt: `${process.env.KAFKA_GRUPO ?? 'reportes'}-dlt`
	},

	concurrencia: entero(process.env.KAFKA_CONCURRENCIA, 3),

	/** Falla de prueba para la demo (como el cliente FALLA del demo): simula que la base de reportes no responde. */
	habitacionFalla: process.env.SIMULAR_FALLO_HABITACION ?? 'HAB-FALLA'
} as const;

export function topicoDlt(topico: string): string {
	return `${topico}${kafkaConfig.sufijoDlt}`;
}
