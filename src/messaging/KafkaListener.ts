// Decorador @KafkaListener (equivalente al de Spring Kafka del demo):
//   @KafkaListener(topics = ..., groupId = "cobros", concurrency = "3")
// Marca un método como consumidor. ContenedorKafka lo suscribe al conectar y se encarga de deserializar,
// validar, reintentar, mandar al DLT y confirmar el offset: el listener solo tiene lógica de negocio.

export interface ContextoRegistro {
	topico: string;
	particion: number;
	offset: string;
	key?: string;
	headers: Record<string, string>;
	/** Número del miembro dentro del grupo (0 .. concurrency-1). */
	miembro: number;
}

export interface OpcionesKafkaListener<T> {
	topics: string[];
	groupId: string;
	/** Miembros del grupo en este proceso. Más que particiones = miembros ociosos (guía 3.3.1). */
	concurrency?: number;
	/** Convierte el JSON al DTO; lanza ErrorNoRecuperable si no es válido. */
	validar?: (cuerpo: unknown) => T;
	/** false en los listeners de DLT: un error ahí se registra y no vuelve a otro DLT. */
	enviarADlt?: boolean;
}

export interface ListenerKafkaRegistrado {
	nombre: string;
	topics: string[];
	groupId: string;
	concurrency: number;
	enviarADlt: boolean;
	validar: (cuerpo: unknown) => unknown;
	manejar: (datos: unknown, contexto: ContextoRegistro) => Promise<void>;
}

const registro: ListenerKafkaRegistrado[] = [];

export function KafkaListener<T>(opciones: OpcionesKafkaListener<T>) {
	return function <This extends object>(
		metodo: (this: This, datos: T, contexto: ContextoRegistro) => Promise<void>,
		contexto: ClassMethodDecoratorContext<This, (this: This, datos: T, contexto: ContextoRegistro) => Promise<void>>
	): void {
		contexto.addInitializer(function (this: This) {
			registro.push({
				nombre: `${this.constructor.name}.${String(contexto.name)}`,
				topics: opciones.topics,
				groupId: opciones.groupId,
				concurrency: Math.max(1, opciones.concurrency ?? 1),
				enviarADlt: opciones.enviarADlt ?? true,
				validar: opciones.validar ?? ((cuerpo) => cuerpo),
				manejar: (datos, ctx) => metodo.call(this, datos as T, ctx)
			});
		});
	};
}

/** Listeners de todas las instancias creadas (cada `new` de una clase con @KafkaListener se registra). */
export function listenersKafkaRegistrados(): readonly ListenerKafkaRegistrado[] {
	return registro;
}
