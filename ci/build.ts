import './scope';
import '@iron-e/scabbard/rust';
import { Container } from '@dagger.io/dagger';
import { enqueue } from '@iron-e/scabbard';
import { WITH_CARGO_HACK } from '@iron-e/scabbard/rust/scope';

enqueue(async (_, inject) => {
	const withCargo = (await inject(WITH_CARGO_HACK)).instance(Container);
	const output = await withCargo.withExecCargoHack('build').stdout();
	console.log(output);
});

await import.meta.filename.runPipelinesIfMain();
