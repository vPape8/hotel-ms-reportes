import { createPool, Pool } from 'mysql2/promise';

/** Pool de MySQL. Sin MYSQL_HOST devuelve undefined y el servicio guarda la proyección en memoria. */
export function crearPoolBaseDatos(): Pool | undefined {
	if (!process.env.MYSQL_HOST) {
		return undefined;
	}

	return createPool({
		host: process.env.MYSQL_HOST,
		port: Number(process.env.MYSQL_PORT ?? 3306),
		user: process.env.MYSQL_USER ?? 'root',
		password: process.env.MYSQL_PASSWORD ?? '',
		database: process.env.MYSQL_DATABASE ?? 'hotel',
		waitForConnections: true,
		connectionLimit: Number(process.env.MYSQL_CONNECTION_LIMIT ?? 5),
		dateStrings: false
	});
}
