import { Request, Response } from 'express';
import { log } from '../logs/logger';
import { FiltroKpis, ReporteRepository } from '../repositories/reporteRepository';

function fecha(valor: unknown, finDelDia: boolean): Date | undefined | null {
	if (valor === undefined || valor === '') return undefined;
	if (typeof valor !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(valor) || Number.isNaN(Date.parse(valor))) return null;
	return new Date(`${valor}T${finDelDia ? '23:59:59.999' : '00:00:00.000'}Z`);
}

export class ReporteController {
	constructor(private readonly repositorio: ReporteRepository) {}

	/** GET /api/v1/reportes/kpis?desde=YYYY-MM-DD&hasta=YYYY-MM-DD (por fecha de entrada). */
	kpis = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const desde = fecha(solicitud.query.desde, false);
		const hasta = fecha(solicitud.query.hasta, true);
		const detalles = [
			...(desde === null ? [{ campo: 'desde', mensaje: 'Debe ser una fecha con formato YYYY-MM-DD' }] : []),
			...(hasta === null ? [{ campo: 'hasta', mensaje: 'Debe ser una fecha con formato YYYY-MM-DD' }] : [])
		];
		if (detalles.length > 0) {
			respuesta.status(400).json({ error: 'VALIDACION_FILTROS', detalles });
			return;
		}
		try {
			const filtro: FiltroKpis = { ...(desde ? { desde } : {}), ...(hasta ? { hasta } : {}) };
			respuesta.json({ data: await this.repositorio.kpis(filtro) });
		} catch (error) {
			log('error', 'ERROR_KPIS', { motivo: error instanceof Error ? error.message : String(error) });
			respuesta.status(500).json({ error: 'ERROR_INTERNO' });
		}
	};
}
