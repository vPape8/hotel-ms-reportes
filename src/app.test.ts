import assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
import test from 'node:test';
import { RequestHandler } from 'express';
import { crearApp } from './app';
import { FalloSimuladoError, ReportesListener } from './consumers/ReportesListener';
import { ErrorNoRecuperable } from './messaging/errores';
import { ReservaEventoDto, validarEventoReserva } from './messaging/eventoReserva';
import { listenersKafkaRegistrados } from './messaging/KafkaListener';
import { InMemoryReporteRepository, Kpis } from './repositories/reporteRepository';

function evento(tipo: ReservaEventoDto['tipo'], estado: ReservaEventoDto['reserva']['estado'], extra: Partial<ReservaEventoDto['reserva']> = {}, ocurridoEn = '2026-11-01T10:00:00.000Z'): ReservaEventoDto {
	return {
		eventoId: `${tipo}-${extra.id ?? 'r1'}-${ocurridoEn}`,
		tipo,
		version: 1,
		ocurridoEn,
		actor: { tipo: 'INVITADO', id: 'invitado', roles: [] },
		origen: { canal: 'WEB_HUESPEDES', ip: '10.0.0.1' },
		reserva: {
			id: 'r1', codigo: 'RES-1', idHabitacion: 'HAB-101', estado,
			fechaEntrada: '2026-12-01T15:00:00.000Z', fechaSalida: '2026-12-03T11:00:00.000Z',
			noches: 2, cantidadHuespedes: 2, precioTotal: 200,
			...extra
		}
	};
}

const contexto = { topico: 'reservas', particion: 0, offset: '0', headers: {}, miembro: 0 };

test('valida el DTO del evento', () => {
	const valido = evento('RESERVA_CREADA', 'PENDIENTE');
	assert.deepEqual(validarEventoReserva(JSON.parse(JSON.stringify(valido))), valido);
	assert.throws(() => validarEventoReserva({ ...valido, tipo: 'OTRO' }), ErrorNoRecuperable);
	assert.throws(() => validarEventoReserva({ ...valido, reserva: { ...valido.reserva, precioTotal: 'abc' } }), ErrorNoRecuperable);
});

test('KPIs a partir de los eventos, idempotencia y falla simulada', async () => {
	const repositorio = new InMemoryReporteRepository();
	new ReportesListener(repositorio);
	const listener = listenersKafkaRegistrados().find((registrado) => registrado.groupId === 'reportes')!;
	assert.equal(listener.concurrency, 3);

	await listener.manejar(evento('RESERVA_CREADA', 'PENDIENTE'), contexto);
	await listener.manejar(evento('RESERVA_CONFIRMADA', 'CONFIRMADA', { estadoAnterior: 'PENDIENTE' }, '2026-11-01T10:30:00.000Z'), contexto);
	await listener.manejar(evento('RESERVA_CREADA', 'PENDIENTE', { id: 'r2', codigo: 'RES-2', idHabitacion: 'HAB-202', precioTotal: 90 }), contexto);
	await listener.manejar(evento('RESERVA_CANCELADA', 'CANCELADA', { id: 'r2', codigo: 'RES-2', idHabitacion: 'HAB-202', precioTotal: 90, estadoAnterior: 'PENDIENTE' }, '2026-11-01T11:00:00.000Z'), contexto);
	// El mismo evento otra vez (reentrega): no se cuenta dos veces.
	await listener.manejar(evento('RESERVA_CREADA', 'PENDIENTE'), contexto);

	const kpis = await repositorio.kpis({});
	assert.equal(kpis.totalReservas, 2);
	assert.equal(kpis.ingresos, 200);
	assert.equal(kpis.nochesVendidas, 2);
	assert.equal(kpis.tasaCancelacion, 50);
	assert.equal(kpis.minutosPromedioConfirmacion, 30);
	assert.deepEqual(kpis.topHabitaciones, [{ idHabitacion: 'HAB-101', reservas: 1, ingresos: 200 }]);
	assert.equal(kpis.ultimaActualizacion, '2026-11-01T11:00:00.000Z');

	await assert.rejects(listener.manejar(evento('RESERVA_CREADA', 'PENDIENTE', { id: 'r3', idHabitacion: 'HAB-FALLA' }), contexto), FalloSimuladoError);
	assert.equal((await repositorio.kpis({})).totalReservas, 2);
});

test('API de KPIs protegida y con filtros validados', async (t) => {
	const repositorio = new InMemoryReporteRepository();
	await repositorio.aplicarEvento(evento('RESERVA_CREADA', 'CONFIRMADA'));
	const autenticacion: RequestHandler = (solicitud, respuesta, siguiente) => {
		if (solicitud.header('authorization') !== 'Bearer admin') {
			respuesta.status(401).json({ error: 'TOKEN_REQUERIDO' });
			return;
		}
		siguiente();
	};
	const servidor = crearApp({ repositorio, autenticacion }).listen(0);
	await new Promise<void>((resolve) => servidor.once('listening', resolve));
	t.after(() => servidor.close());
	const base = `http://127.0.0.1:${(servidor.address() as AddressInfo).port}`;
	const admin = { headers: { Authorization: 'Bearer admin' } };

	assert.equal((await fetch(`${base}/api/v1/reportes/kpis`)).status, 401);
	const ok = await fetch(`${base}/api/v1/reportes/kpis?desde=2026-12-01&hasta=2026-12-31`, admin);
	assert.equal(ok.status, 200);
	assert.equal(((await ok.json()) as { data: Kpis }).data.totalReservas, 1);
	const fuera = await fetch(`${base}/api/v1/reportes/kpis?desde=2027-01-01`, admin);
	assert.equal(((await fuera.json()) as { data: Kpis }).data.totalReservas, 0);
	assert.equal((await fetch(`${base}/api/v1/reportes/kpis?desde=01-12-2026`, admin)).status, 400);
});
