import { NOMBRE_SERVICIO } from '../config/servicio';

type Nivel = 'info' | 'warn' | 'error';

/** Log estructurado (una línea JSON) para seguir los eventos en `docker compose logs`. */
export function log(nivel: Nivel, evento: string, datos: Record<string, unknown> = {}): void {
	const linea = JSON.stringify({ fecha: new Date().toISOString(), servicio: NOMBRE_SERVICIO, nivel, evento, ...datos });
	if (nivel === 'error') {
		console.error(linea);
	} else if (nivel === 'warn') {
		console.warn(linea);
	} else {
		console.log(linea);
	}
}
