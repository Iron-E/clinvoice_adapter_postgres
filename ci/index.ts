// register pipelines
import './build';
import './test';

// run pipelines
await import.meta.filename.runPipelinesIfMain();
