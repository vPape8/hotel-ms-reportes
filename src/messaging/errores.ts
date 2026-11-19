/**
 * Error permanente: reintentar no lo arregla (payload inválido, dato mal formado).
 * El contenedor lo manda directo al DLT, sin reintentos (como IllegalArgumentException en la guía 3.3.3).
 * Cualquier otro error se trata como temporal y se reintenta (como IllegalStateException).
 */
export class ErrorNoRecuperable extends Error {
	constructor(mensaje: string) {
		super(mensaje);
		this.name = 'ErrorNoRecuperable';
	}
}
