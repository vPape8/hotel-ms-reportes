import 'dotenv/config';
import { crearApp } from './app';
import { crearPoolBaseDatos } from './config/database';
import { kafkaConfig } from './config/kafka.config';
import { DltListener } from './consumers/DltListener';
import { ReportesListener } from './consumers/ReportesListener';
import { log } from './logs/logger';
import { ContenedorKafka } from './messaging/ContenedorKafka';
import { listenersKafkaRegistrados } from './messaging/KafkaListener';
import { InMemoryReporteRepository, MySqlReporteRepository, ReporteRepository } from './repositories/reporteRepository';

const puerto = Number(process.env.PORT ?? 8086);
const pool = crearPoolBaseDatos();
const repositorio: ReporteRepository = pool
	? new MySqlReporteRepository(pool, kafkaConfig.grupos.reportes)
	: new InMemoryReporteRepository();
if (!pool) {
	log('warn', 'PROYECCION_EN_MEMORIA', { detalle: 'MYSQL_HOST no definido' });
}

// Cada instancia registra sus métodos @KafkaListener; el contenedor los suscribe al conectar.
new ReportesListener(repositorio);
new DltListener();

const contenedor = kafkaConfig.brokers.length > 0
	? new ContenedorKafka([...kafkaConfig.brokers], listenersKafkaRegistrados)
	: undefined;
if (!contenedor) {
	log('warn', 'KAFKA_DESACTIVADO', { detalle: 'KAFKA_BROKERS no definido' });
}

const servidor = crearApp({ repositorio, kafkaConectado: () => contenedor?.estaConectado ?? false }).listen(puerto, () => {
	log('info', 'SERVICIO_INICIADO', { puerto });
	void contenedor?.iniciar();
});

for (const senal of ['SIGINT', 'SIGTERM'] as const) {
	process.once(senal, () => {
		servidor.close();
		void Promise.all([contenedor?.detener(), pool?.end()]).finally(() => process.exit(0));
	});
}
