import { ResultSetHeader, RowDataPacket } from 'mysql2';
import { Pool, PoolConnection } from 'mysql2/promise';
import { EstadoReserva, ReservaEventoDto } from '../messaging/eventoReserva';

export interface FiltroKpis {
	/** Fecha de entrada desde (inclusive). */
	desde?: Date;
	/** Fecha de entrada hasta (inclusive). */
	hasta?: Date;
}

export interface Kpis {
	totalReservas: number;
	porEstado: { estado: EstadoReserva; cantidad: number }[];
	/** Reservas confirmadas o completadas. */
	ingresos: number;
	nochesVendidas: number;
	huespedes: number;
	/** Porcentaje de reservas canceladas (0–100). */
	tasaCancelacion: number;
	/** Minutos promedio entre que se crea una reserva y se confirma. */
	minutosPromedioConfirmacion: number | null;
	porCanal: { canal: string; cantidad: number }[];
	topHabitaciones: { idHabitacion: string; reservas: number; ingresos: number }[];
	/** Cuándo llegó el último evento: el panel lo usa para saber si hay datos nuevos. */
	ultimaActualizacion: string | null;
}

export type ResultadoAplicar = 'APLICADO' | 'DUPLICADO';

export interface ReporteRepository {
	/** Aplica el evento a la proyección. Idempotente: el mismo eventoId solo cuenta una vez. */
	aplicarEvento(evento: ReservaEventoDto): Promise<ResultadoAplicar>;
	kpis(filtro: FiltroKpis): Promise<Kpis>;
}

interface Proyeccion {
	reservaId: string;
	codigo: string;
	idHabitacion: string;
	estado: EstadoReserva;
	canal: string;
	fechaEntrada: Date;
	noches: number;
	cantidadHuespedes: number;
	precioTotal: number;
	creadaEn: Date;
	confirmadaEn: Date | null;
	actualizadaEn: Date;
}

const ESTADOS_CON_INGRESO: readonly EstadoReserva[] = ['CONFIRMADA', 'COMPLETADA'];

function confirmadaEn(evento: ReservaEventoDto): Date | null {
	return evento.reserva.estado === 'CONFIRMADA' ? new Date(evento.ocurridoEn) : null;
}

/** Calcula los KPIs a partir de la proyección (lo usa la versión en memoria). */
function calcularKpis(filas: Proyeccion[]): Kpis {
	const conIngreso = filas.filter((fila) => ESTADOS_CON_INGRESO.includes(fila.estado));
	const confirmaciones = filas.filter((fila) => fila.confirmadaEn);
	const contar = <K extends string>(clave: (fila: Proyeccion) => K) => {
		const mapa = new Map<K, number>();
		for (const fila of filas) mapa.set(clave(fila), (mapa.get(clave(fila)) ?? 0) + 1);
		return [...mapa.entries()].sort((a, b) => b[1] - a[1]);
	};
	const porHabitacion = new Map<string, { reservas: number; ingresos: number }>();
	for (const fila of conIngreso) {
		const actual = porHabitacion.get(fila.idHabitacion) ?? { reservas: 0, ingresos: 0 };
		porHabitacion.set(fila.idHabitacion, { reservas: actual.reservas + 1, ingresos: actual.ingresos + fila.precioTotal });
	}
	const ultima = filas.reduce<Date | null>((max, fila) => (!max || fila.actualizadaEn > max ? fila.actualizadaEn : max), null);

	return {
		totalReservas: filas.length,
		porEstado: contar((fila) => fila.estado).map(([estado, cantidad]) => ({ estado, cantidad })),
		ingresos: conIngreso.reduce((suma, fila) => suma + fila.precioTotal, 0),
		nochesVendidas: conIngreso.reduce((suma, fila) => suma + fila.noches, 0),
		huespedes: conIngreso.reduce((suma, fila) => suma + fila.cantidadHuespedes, 0),
		tasaCancelacion: filas.length === 0 ? 0 : Math.round((filas.filter((fila) => fila.estado === 'CANCELADA').length / filas.length) * 1000) / 10,
		minutosPromedioConfirmacion: confirmaciones.length === 0
			? null
			: Math.round(confirmaciones.reduce((suma, fila) => suma + (fila.confirmadaEn!.getTime() - fila.creadaEn.getTime()), 0) / confirmaciones.length / 60000 * 10) / 10,
		porCanal: contar((fila) => fila.canal).map(([canal, cantidad]) => ({ canal, cantidad })),
		topHabitaciones: [...porHabitacion.entries()]
			.map(([idHabitacion, valores]) => ({ idHabitacion, ...valores }))
			.sort((a, b) => b.ingresos - a.ingresos)
			.slice(0, 5),
		ultimaActualizacion: ultima?.toISOString() ?? null
	};
}

/** Para tests y desarrollo sin MySQL. */
export class InMemoryReporteRepository implements ReporteRepository {
	readonly procesados = new Set<string>();
	readonly proyeccion = new Map<string, Proyeccion>();

	async aplicarEvento(evento: ReservaEventoDto): Promise<ResultadoAplicar> {
		if (this.procesados.has(evento.eventoId)) return 'DUPLICADO';
		this.procesados.add(evento.eventoId);
		const anterior = this.proyeccion.get(evento.reserva.id);
		const r = evento.reserva;
		this.proyeccion.set(r.id, {
			reservaId: r.id,
			codigo: r.codigo,
			idHabitacion: r.idHabitacion,
			estado: r.estado,
			canal: anterior?.canal ?? evento.origen.canal,
			fechaEntrada: new Date(r.fechaEntrada),
			noches: r.noches,
			cantidadHuespedes: r.cantidadHuespedes,
			precioTotal: r.precioTotal,
			creadaEn: anterior?.creadaEn ?? new Date(evento.ocurridoEn),
			confirmadaEn: anterior?.confirmadaEn ?? confirmadaEn(evento),
			actualizadaEn: new Date(evento.ocurridoEn)
		});
		return 'APLICADO';
	}

	async kpis(filtro: FiltroKpis): Promise<Kpis> {
		return calcularKpis([...this.proyeccion.values()].filter((fila) =>
			(!filtro.desde || fila.fechaEntrada >= filtro.desde) && (!filtro.hasta || fila.fechaEntrada <= filtro.hasta)));
	}
}

const ESQUEMA = [
	// Idempotencia (guía 3.3.3): un evento ya aplicado no se vuelve a sumar.
	`CREATE TABLE IF NOT EXISTS processed_messages (
		consumer_group VARCHAR(100) NOT NULL,
		evento_id CHAR(36) NOT NULL,
		procesado_en DATETIME(3) NOT NULL,
		PRIMARY KEY (consumer_group, evento_id)
	)`,
	// Proyección de lectura: el último estado de cada reserva según los eventos. No es la tabla de ms-reservas.
	`CREATE TABLE IF NOT EXISTS reportes_reservas (
		reserva_id CHAR(36) PRIMARY KEY,
		codigo VARCHAR(64) NOT NULL,
		id_habitacion VARCHAR(64) NOT NULL,
		estado VARCHAR(20) NOT NULL,
		canal VARCHAR(30) NOT NULL,
		fecha_entrada DATETIME(3) NOT NULL,
		noches INT NOT NULL,
		cantidad_huespedes INT NOT NULL,
		precio_total DECIMAL(12, 2) NOT NULL,
		creada_en DATETIME(3) NOT NULL,
		confirmada_en DATETIME(3) NULL,
		actualizada_en DATETIME(3) NOT NULL,
		INDEX idx_reportes_fecha_entrada (fecha_entrada)
	)`
];

interface FilaResumen extends RowDataPacket {
	total: number;
	canceladas: string | null;
	ingresos: string | null;
	noches: string | null;
	huespedes: string | null;
	minutos_confirmacion: string | null;
	ultima: Date | null;
}

export class MySqlReporteRepository implements ReporteRepository {
	private esquemaListo?: Promise<void>;

	constructor(private readonly pool: Pool, private readonly consumerGroup: string) {}

	private asegurarEsquema(): Promise<void> {
		this.esquemaListo ??= (async () => {
			for (const sentencia of ESQUEMA) await this.pool.query(sentencia);
		})().catch((error: unknown) => {
			this.esquemaListo = undefined;
			throw error;
		});
		return this.esquemaListo;
	}

	async aplicarEvento(evento: ReservaEventoDto): Promise<ResultadoAplicar> {
		await this.asegurarEsquema();
		const conexion = await this.pool.getConnection();
		try {
			await conexion.beginTransaction();
			const [marca] = await conexion.execute<ResultSetHeader>(
				'INSERT IGNORE INTO processed_messages (consumer_group, evento_id, procesado_en) VALUES (?, ?, ?)',
				[this.consumerGroup, evento.eventoId, new Date()]
			);
			if (marca.affectedRows === 0) {
				await conexion.rollback();
				return 'DUPLICADO';
			}
			await this.upsert(conexion, evento);
			await conexion.commit();
			return 'APLICADO';
		} catch (error) {
			await conexion.rollback().catch(() => undefined);
			throw error;
		} finally {
			conexion.release();
		}
	}

	private async upsert(conexion: PoolConnection, evento: ReservaEventoDto): Promise<void> {
		const r = evento.reserva;
		const ocurrido = new Date(evento.ocurridoEn);
		// canal y creada_en se guardan con el primer evento; confirmada_en, con la primera confirmación.
		await conexion.execute(
			`INSERT INTO reportes_reservas (reserva_id, codigo, id_habitacion, estado, canal, fecha_entrada, noches,
				cantidad_huespedes, precio_total, creada_en, confirmada_en, actualizada_en)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS nuevo
			ON DUPLICATE KEY UPDATE
				estado = nuevo.estado,
				precio_total = nuevo.precio_total,
				confirmada_en = COALESCE(reportes_reservas.confirmada_en, nuevo.confirmada_en),
				actualizada_en = nuevo.actualizada_en`,
			[r.id, r.codigo, r.idHabitacion, r.estado, evento.origen.canal, new Date(r.fechaEntrada), r.noches,
				r.cantidadHuespedes, r.precioTotal, ocurrido, confirmadaEn(evento), ocurrido]
		);
	}

	async kpis(filtro: FiltroKpis): Promise<Kpis> {
		await this.asegurarEsquema();
		const condiciones: string[] = [];
		const valores: unknown[] = [];
		if (filtro.desde) { condiciones.push('fecha_entrada >= ?'); valores.push(filtro.desde); }
		if (filtro.hasta) { condiciones.push('fecha_entrada <= ?'); valores.push(filtro.hasta); }
		const donde = condiciones.length > 0 ? `WHERE ${condiciones.join(' AND ')}` : '';
		const conIngreso = `estado IN ('CONFIRMADA', 'COMPLETADA')`;

		const [[resumen]] = await this.pool.query<FilaResumen[]>(
			`SELECT COUNT(*) AS total,
				SUM(estado = 'CANCELADA') AS canceladas,
				SUM(CASE WHEN ${conIngreso} THEN precio_total ELSE 0 END) AS ingresos,
				SUM(CASE WHEN ${conIngreso} THEN noches ELSE 0 END) AS noches,
				SUM(CASE WHEN ${conIngreso} THEN cantidad_huespedes ELSE 0 END) AS huespedes,
				AVG(TIMESTAMPDIFF(SECOND, creada_en, confirmada_en)) / 60 AS minutos_confirmacion,
				MAX(actualizada_en) AS ultima
			FROM reportes_reservas ${donde}`, valores);
		const [porEstado] = await this.pool.query<RowDataPacket[]>(
			`SELECT estado, COUNT(*) AS cantidad FROM reportes_reservas ${donde} GROUP BY estado ORDER BY cantidad DESC`, valores);
		const [porCanal] = await this.pool.query<RowDataPacket[]>(
			`SELECT canal, COUNT(*) AS cantidad FROM reportes_reservas ${donde} GROUP BY canal ORDER BY cantidad DESC`, valores);
		const [top] = await this.pool.query<RowDataPacket[]>(
			`SELECT id_habitacion, COUNT(*) AS reservas, SUM(precio_total) AS ingresos FROM reportes_reservas
			${donde ? `${donde} AND` : 'WHERE'} ${conIngreso}
			GROUP BY id_habitacion ORDER BY ingresos DESC LIMIT 5`, valores);

		const total = Number(resumen?.total ?? 0);
		return {
			totalReservas: total,
			porEstado: porEstado.map((fila) => ({ estado: fila.estado as EstadoReserva, cantidad: Number(fila.cantidad) })),
			ingresos: Number(resumen?.ingresos ?? 0),
			nochesVendidas: Number(resumen?.noches ?? 0),
			huespedes: Number(resumen?.huespedes ?? 0),
			tasaCancelacion: total === 0 ? 0 : Math.round((Number(resumen?.canceladas ?? 0) / total) * 1000) / 10,
			minutosPromedioConfirmacion: resumen?.minutos_confirmacion === null || resumen?.minutos_confirmacion === undefined
				? null
				: Math.round(Number(resumen.minutos_confirmacion) * 10) / 10,
			porCanal: porCanal.map((fila) => ({ canal: String(fila.canal), cantidad: Number(fila.cantidad) })),
			topHabitaciones: top.map((fila) => ({ idHabitacion: String(fila.id_habitacion), reservas: Number(fila.reservas), ingresos: Number(fila.ingresos) })),
			ultimaActualizacion: resumen?.ultima ? new Date(resumen.ultima).toISOString() : null
		};
	}
}
