import { Consumer, IHeaders, Kafka, logLevel, Partitioners, Producer } from 'kafkajs';
import { kafkaConfig, topicoDlt } from '../config/kafka.config';
import { log } from '../logs/logger';
import { ListenerKafkaRegistrado } from './KafkaListener';
import { MotivoDlt, OperacionesRegistro, procesarRegistro, RegistroKafka } from './procesarRegistro';

function headersATexto(headers: IHeaders | undefined): Record<string, string> {
	const resultado: Record<string, string> = {};
	for (const [nombre, valor] of Object.entries(headers ?? {})) {
		const unico = Array.isArray(valor) ? valor[0] : valor;
		if (unico !== undefined) {
			resultado[nombre] = Buffer.isBuffer(unico) ? unico.toString('utf8') : String(unico);
		}
	}
	return resultado;
}

/**
 * Conecta los @KafkaListener: por cada listener crea "concurrency" consumidores con el mismo groupId
 * (como concurrency = "3" en el demo), así Kafka reparte las particiones entre ellos.
 * También tiene el productor que publica en los DLT. Si Kafka no está, reintenta solo.
 */
export class ContenedorKafka {
	private readonly kafka: Kafka;
	private consumidores: Consumer[] = [];
	private productorDlt?: Producer;
	private detenido = false;
	private conectado = false;

	constructor(brokers: string[], private readonly listeners: () => readonly ListenerKafkaRegistrado[]) {
		this.kafka = new Kafka({ clientId: kafkaConfig.clientId, brokers, logLevel: logLevel.WARN });
	}

	get estaConectado(): boolean {
		return this.conectado;
	}

	async iniciar(): Promise<void> {
		this.detenido = false;
		try {
			await this.conectar();
		} catch (error) {
			log('error', 'KAFKA_CONEXION_FALLIDA', { motivo: error instanceof Error ? error.message : String(error), reintentoEnMs: kafkaConfig.reconexionMs });
			await this.desconectar();
			this.programarReintento();
		}
	}

	async detener(): Promise<void> {
		this.detenido = true;
		await this.desconectar();
	}

	private programarReintento(): void {
		if (!this.detenido) {
			setTimeout(() => void this.iniciar(), kafkaConfig.reconexionMs);
		}
	}

	private async conectar(): Promise<void> {
		const productor = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
		await productor.connect();
		this.productorDlt = productor;

		const operaciones: OperacionesRegistro = {
			enviarADlt: (registro, listener, motivo) => this.enviarADlt(registro, listener, motivo),
			esperar: (ms) => new Promise((resolve) => setTimeout(resolve, ms))
		};

		for (const listener of this.listeners()) {
			for (let miembro = 0; miembro < listener.concurrency; miembro++) {
				const consumidor = this.kafka.consumer({
					groupId: listener.groupId,
					sessionTimeout: kafkaConfig.consumidor.sessionTimeoutMs,
					heartbeatInterval: kafkaConfig.consumidor.heartbeatIntervalMs
				});
				// Si kafkajs se rinde (sin reinicio automático), se reconecta todo el contenedor.
				consumidor.on(consumidor.events.CRASH, (evento) => {
					if (!evento.payload.restart && !this.detenido) {
						log('error', 'CONSUMIDOR_CAIDO', { grupo: listener.groupId, miembro, motivo: evento.payload.error.message });
						void this.desconectar().then(() => this.programarReintento());
					}
				});
				await consumidor.connect();
				await consumidor.subscribe({ topics: listener.topics, fromBeginning: kafkaConfig.consumidor.fromBeginning });
				await consumidor.run({
					// El offset se confirma cuando eachMessage termina sin lanzar error.
					eachMessage: async ({ topic, partition, message }) => {
						const registro: RegistroKafka = {
							topico: topic,
							particion: partition,
							offset: message.offset,
							key: message.key?.toString('utf8') ?? null,
							valor: message.value,
							headers: headersATexto(message.headers)
						};
						await procesarRegistro(registro, listener, miembro, kafkaConfig.reintentos, operaciones);
					}
				});
				this.consumidores.push(consumidor);
			}
		}

		this.conectado = true;
		log('info', 'KAFKA_CONECTADO', {
			brokers: kafkaConfig.brokers,
			listeners: this.listeners().map((listener) => `${listener.nombre} <- ${listener.topics.join(',')} (grupo ${listener.groupId}, concurrency ${listener.concurrency})`)
		});
	}

	private async desconectar(): Promise<void> {
		this.conectado = false;
		const consumidores = this.consumidores;
		this.consumidores = [];
		await Promise.all(consumidores.map((consumidor) => consumidor.disconnect().catch(() => undefined)));
		await this.productorDlt?.disconnect().catch(() => undefined);
		this.productorDlt = undefined;
	}

	/**
	 * Publica el mensaje original en <tópico>.DLT, en la misma partición, con headers que explican
	 * por qué murió (los mismos nombres que agrega Spring: kafka_exception-message, kafka_original-topic...).
	 */
	private async enviarADlt(registro: RegistroKafka, listener: ListenerKafkaRegistrado, motivo: MotivoDlt): Promise<void> {
		if (!this.productorDlt) {
			throw new Error('Sin conexión con Kafka para publicar en el DLT');
		}
		const destino = topicoDlt(registro.topico);
		await this.productorDlt.send({
			topic: destino,
			acks: -1,
			messages: [{
				key: registro.key,
				value: registro.valor,
				partition: registro.particion,
				headers: {
					...registro.headers,
					'kafka_exception-fqcn': motivo.excepcion,
					'kafka_exception-message': motivo.mensaje,
					'kafka_original-topic': registro.topico,
					'kafka_original-partition': String(registro.particion),
					'kafka_original-offset': registro.offset,
					'kafka_original-consumer-group': listener.groupId,
					'kafka_intentos': String(motivo.intentos)
				}
			}]
		});
		log('error', 'ENVIADO_A_DLT', { grupo: listener.groupId, topic: destino, particion: registro.particion, offsetOriginal: registro.offset, key: registro.key, motivo: motivo.mensaje, intentos: motivo.intentos });
	}
}
