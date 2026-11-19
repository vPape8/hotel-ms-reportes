import assert from 'node:assert/strict';
import test from 'node:test';
import { ErrorNoRecuperable } from './errores';
import { ListenerKafkaRegistrado } from './KafkaListener';
import { MotivoDlt, OperacionesRegistro, procesarRegistro, RegistroKafka } from './procesarRegistro';

const politica = { intentos: 3, esperaMs: 1000 };

function registro(valor: string): RegistroKafka {
	return { topico: 'reservas', particion: 1, offset: '7', key: 'HAB-101', valor: Buffer.from(valor), headers: {} };
}

function listener(manejar: ListenerKafkaRegistrado['manejar'], extra: Partial<ListenerKafkaRegistrado> = {}): ListenerKafkaRegistrado {
	return { nombre: 'Prueba.metodo', topics: ['reservas'], groupId: 'reportes', concurrency: 1, enviarADlt: true, validar: (cuerpo) => cuerpo, manejar, ...extra };
}

function operaciones() {
	const dlt: MotivoDlt[] = [];
	const esperas: number[] = [];
	const ops: OperacionesRegistro = {
		enviarADlt: async (_registro, _listener, motivo) => { dlt.push(motivo); },
		esperar: async (ms) => { esperas.push(ms); }
	};
	return { ops, dlt, esperas };
}

test('caso feliz: procesa una vez y no toca el DLT', async () => {
	const { ops, dlt } = operaciones();
	let llamadas = 0;
	assert.equal(await procesarRegistro(registro('{"a":1}'), listener(async () => { llamadas++; }), 0, politica, ops), 'OK');
	assert.equal(llamadas, 1);
	assert.equal(dlt.length, 0);
});

test('payload inválido: directo al DLT, sin reintentos (paso 5 de la guía 3.3.3)', async () => {
	const { ops, dlt, esperas } = operaciones();
	let llamadas = 0;
	const validar = () => { throw new ErrorNoRecuperable('"precioTotal" debe ser un número'); };
	assert.equal(await procesarRegistro(registro('{}'), listener(async () => { llamadas++; }, { validar }), 0, politica, ops), 'DLT');
	assert.equal(llamadas, 0);
	assert.equal(esperas.length, 0);
	assert.equal(dlt[0]?.intentos, 1);
});

test('JSON roto: directo al DLT', async () => {
	const { ops, dlt } = operaciones();
	assert.equal(await procesarRegistro(registro('ord-malo|Juan|abc'), listener(async () => {}), 0, politica, ops), 'DLT');
	assert.equal(dlt[0]?.mensaje, 'El valor no es JSON válido');
});

test('fallo temporal: 3 intentos con 1 s de espera y después DLT (paso 6)', async () => {
	const { ops, dlt, esperas } = operaciones();
	let llamadas = 0;
	const resultado = await procesarRegistro(registro('{}'), listener(async () => { llamadas++; throw new Error('base caída'); }), 0, politica, ops);
	assert.equal(resultado, 'DLT');
	assert.equal(llamadas, 3);
	assert.deepEqual(esperas, [1000, 1000]);
	assert.deepEqual(dlt[0], { excepcion: 'Error', mensaje: 'base caída', intentos: 3 });
});

test('fallo temporal que se recupera en el segundo intento: no va al DLT', async () => {
	const { ops, dlt } = operaciones();
	let llamadas = 0;
	const resultado = await procesarRegistro(registro('{}'), listener(async () => { llamadas++; if (llamadas === 1) throw new Error('timeout'); }), 0, politica, ops);
	assert.equal(resultado, 'OK');
	assert.equal(llamadas, 2);
	assert.equal(dlt.length, 0);
});

test('si publicar en el DLT falla, lanza el error para que Kafka no confirme el offset', async () => {
	const ops: OperacionesRegistro = { enviarADlt: async () => { throw new Error('sin brokers'); }, esperar: async () => {} };
	await assert.rejects(procesarRegistro(registro('roto'), listener(async () => {}), 0, politica, ops), /sin brokers/);
});

test('un listener de DLT acepta texto y nunca reenvía a otro DLT', async () => {
	const { ops, dlt } = operaciones();
	let recibido: unknown;
	await procesarRegistro(registro('ord-malo|Juan|abc'), listener(async (datos) => { recibido = datos; }, { enviarADlt: false }), 0, politica, ops);
	assert.equal(recibido, 'ord-malo|Juan|abc');
	assert.equal(await procesarRegistro(registro('{}'), listener(async () => { throw new Error('x'); }, { enviarADlt: false }), 0, politica, ops), 'ERROR_EN_DLT');
	assert.equal(dlt.length, 0);
});
