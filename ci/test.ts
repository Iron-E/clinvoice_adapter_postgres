import './scope';
import '@iron-e/scabbard/db';
import '@iron-e/scabbard/rust';
import { Container } from '@dagger.io/dagger';
import { enqueue } from '@iron-e/scabbard';
import { WITH_CARGO_HACK } from '@iron-e/scabbard/rust/scope';

enqueue(async (client, inject) => {
	const postgres = client.dbService('postgres:16.2', {
		env: { POSTGRES_DB: 'winvoice-adapter', POSTGRES_PASSWORD: 'password', POSTGRES_USER: 'user' },
		initScriptDirs: { 'src/schema/initializable': '/docker-entrypoint-initdb.d' },
	});

	const withCargo = (await inject(WITH_CARGO_HACK)).instance(Container);
	const output = await withCargo
		.withServiceBinding('db', postgres)
		.withEnvVariable('DATABASE_URL', 'postgresql://user:password@db/winvoice-adapter')
		.withEnvVariable('RUSTFLAGS', "-C target-feature=-crt-static")
		.withExecCargoHack('test')
		.stdout()
		;

	console.log(output);
});

await import.meta.filename.runPipelinesIfMain();
