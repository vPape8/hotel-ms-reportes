import { ErrorNoRecuperable } from './errores';

// Contrato del tópico "reservas": lo publica hotel-ms-reservas (src/messaging/kafka/eventoReserva.ts).

export type EstadoReserva = 'PENDIENTE' | 'CONFIRMADA' | 'CANCELADA' | 'COMPLETADA';
export type TipoEventoReserva = 'RESERVA_CREADA' | 'RESERVA_CONFIRMADA' | 'RESERVA_CANCELADA' | 'RESERVA_COMPLETADA';

const ESTADOS: readonly EstadoReserva[] = ['PENDIENTE', 'CONFIRMADA', 'CANCELADA', 'COMPLETADA'];
const TIPOS: readonly TipoEventoReserva[] = ['RESERVA_CREADA', 'RESERVA_CONFIRMADA', 'RESERVA_CANCELADA', 'RESERVA_COMPLETADA'];

export interface ReservaEventoDto {
	eventoId: string;
	tipo: TipoEventoReserva;
	version: number;
	ocurridoEn: string;
	actor: { tipo: 'INVITADO' | 'CLIENTE' | 'INTERNO'; id: string; nombre?: string; roles: string[] };
	origen: { canal: string; ip?: string };
	reserva: {
		id: string;
		codigo: string;
		idHabitacion: string;
		estado: EstadoReserva;
		estadoAnterior?: EstadoReserva;
		fechaEntrada: string;
		fechaSalida: string;
		noches: number;
		cantidadHuespedes: number;
		precioTotal: number;
	};
}

function objeto(valor: unknown, campo: string): Record<string, unknown> {
	if (typeof valor !== 'object' || valor === null || Array.isArray(valor)) {
		throw new ErrorNoRecuperable(`"${campo}" debe ser un objeto`);
	}
	return valor as Record<string, unknown>;
}

function texto(origen: Record<string, unknown>, campo: string): string {
	const valor = origen[campo];
	if (typeof valor !== 'string' || !valor.trim()) {
		throw new ErrorNoRecuperable(`"${campo}" ausente o vacío`);
	}
	return valor;
}

function numero(origen: Record<string, unknown>, campo: string, minimo: number): number {
	const valor = origen[campo];
	if (typeof valor !== 'number' || !Number.isFinite(valor) || valor < minimo) {
		throw new ErrorNoRecuperable(`"${campo}" debe ser un número mayor o igual a ${minimo}`);
	}
	return valor;
}

function fecha(origen: Record<string, unknown>, campo: string): string {
	const valor = texto(origen, campo);
	if (Number.isNaN(Date.parse(valor))) {
		throw new ErrorNoRecuperable(`"${campo}" no es una fecha válida`);
	}
	return valor;
}

function opcion<T extends string>(origen: Record<string, unknown>, campo: string, opciones: readonly T[]): T {
	const valor = origen[campo];
	if (!opciones.includes(valor as T)) {
		throw new ErrorNoRecuperable(`"${campo}" debe ser uno de: ${opciones.join(', ')}`);
	}
	return valor as T;
}

/** Valida el JSON recibido contra el DTO. Si no cumple, el evento va directo al DLT. */
export function validarEventoReserva(cuerpo: unknown): ReservaEventoDto {
	const evento = objeto(cuerpo, 'evento');
	const actor = objeto(evento.actor, 'actor');
	const origen = objeto(evento.origen, 'origen');
	const reserva = objeto(evento.reserva, 'reserva');

	return {
		eventoId: texto(evento, 'eventoId'),
		tipo: opcion(evento, 'tipo', TIPOS),
		version: numero(evento, 'version', 1),
		ocurridoEn: fecha(evento, 'ocurridoEn'),
		actor: {
			tipo: opcion(actor, 'tipo', ['INVITADO', 'CLIENTE', 'INTERNO'] as const),
			id: texto(actor, 'id'),
			...(typeof actor.nombre === 'string' ? { nombre: actor.nombre } : {}),
			roles: Array.isArray(actor.roles) ? actor.roles.filter((rol): rol is string => typeof rol === 'string') : []
		},
		origen: {
			canal: texto(origen, 'canal'),
			...(typeof origen.ip === 'string' ? { ip: origen.ip } : {})
		},
		reserva: {
			id: texto(reserva, 'id'),
			codigo: texto(reserva, 'codigo'),
			idHabitacion: texto(reserva, 'idHabitacion'),
			estado: opcion(reserva, 'estado', ESTADOS),
			...(reserva.estadoAnterior !== undefined ? { estadoAnterior: opcion(reserva, 'estadoAnterior', ESTADOS) } : {}),
			fechaEntrada: fecha(reserva, 'fechaEntrada'),
			fechaSalida: fecha(reserva, 'fechaSalida'),
			noches: numero(reserva, 'noches', 1),
			cantidadHuespedes: numero(reserva, 'cantidadHuespedes', 1),
			precioTotal: numero(reserva, 'precioTotal', 0)
		}
	};
}
