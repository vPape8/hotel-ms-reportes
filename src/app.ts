import express, { Express, NextFunction, Request, RequestHandler, Response } from 'express';
import { ReporteController } from './controllers/reporteController';
import { entraAuthMiddleware } from './middlewares/authMiddleware';
import { ReporteRepository } from './repositories/reporteRepository';

export interface OpcionesApp {
	repositorio: ReporteRepository;
	autenticacion?: RequestHandler;
	/** Estado de los consumidores de Kafka para /health. */
	kafkaConectado?: () => boolean;
}

export function crearApp(opciones: OpcionesApp): Express {
	const controlador = new ReporteController(opciones.repositorio);
	const autenticacion = opciones.autenticacion ?? entraAuthMiddleware;
	const app = express();

	app.get('/health', (_solicitud, respuesta) => {
		const kafka = opciones.kafkaConectado?.() ?? false;
		respuesta.json({ status: 'ok', kafka: kafka ? 'conectado' : 'desconectado' });
	});

	// KPIs: solo Admin (el middleware exige el app role ADMIN, salvo ENTRA_EXIGIR_ROL=false).
	app.get('/api/v1/reportes/kpis', autenticacion, controlador.kpis);

	app.use((_solicitud, respuesta) => {
		respuesta.status(404).json({ error: 'RUTA_NO_ENCONTRADA' });
	});

	app.use((_error: unknown, _solicitud: Request, respuesta: Response, _siguiente: NextFunction) => {
		respuesta.status(500).json({ error: 'ERROR_INTERNO' });
	});

	return app;
}
