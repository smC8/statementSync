import { Worker } from '@temporalio/worker';
import * as activities from './activities'; // Import all your activities
import path from 'path'; // Import the 'path' module

async function run() {
  // Create a Temporal Worker instance.
  const worker = await Worker.create({
    // `workflowsPath` points to the directory containing your workflow definitions.
    // It's crucial to resolve this path correctly for production builds (e.g., `dist/workflows`).
    // Corrected: Use path.join(__dirname, './workflows') to resolve the path relative to the worker's location.
    workflowsPath: path.join(__dirname, './workflows/fetchAndSyncAccountStatements.js'),
    // Pass the activities object directly to the worker.
    activities,
    // The task queue this worker will listen on. Workflows must be started on this queue.
    taskQueue: 'account-statement-sync',
    // Optional: Configure connection to Temporal server if not using default localhost:7233
    // connection: {
    //   address: process.env.TEMPORAL_GRPC_ENDPOINT || 'localhost:7233',
    //   tls: process.env.TEMPORAL_TLS_ENABLE === 'true' ? {
    //     clientCert: Buffer.from(process.env.TEMPORAL_TLS_CERT_PEM || ''),
    //     clientKey: Buffer.from(process.env.TEMPORAL_TLS_KEY_PEM || ''),
    //     serverNameOverride: process.env.TEMPORAL_TLS_SERVER_NAME_OVERRIDE || undefined,
    //   } : false,
    // },
    // Adjust concurrency limits based on your server's resources and activity load
    // maxConcurrentActivityTaskExecutions: 100,
    // maxConcurrentWorkflowTaskExecutions: 100,
  });

  console.log('Temporal Worker started and listening for tasks on "account-statement-sync" task queue...');
  // Start the worker and keep it running.
  await worker.run();
  console.log('Temporal Worker stopped.');
}

// Run the worker. Catch any errors and exit the process.
run().catch((err) => {
  console.error('Temporal Worker crashed:', err);
  process.exit(1);
});
