import { WorkflowClient, WorkflowExecutionAlreadyStartedError } from '@temporalio/client';
import { fetchAndSyncAccountStatements } from './workflows/fetchAndSyncAccountStatements'; // Import the workflow type

async function startSyncWorkflows() {
  // Create a Temporal WorkflowClient instance.
  const client = new WorkflowClient();

  // Define the bank accounts you want to synchronize.
  // In a real application, this list might come from a database,
  // a configuration service, or be triggered by user actions.
  const bankAccountsToSync = [
    { bankId: 'bankA', accountId: '12345' },
    { bankId: 'bankB', accountId: '67890', initialCursor: 'last_successful_cursor_for_bankB' }, // Example: Starting with a known cursor
    { bankId: 'bankC', accountId: '11223' },
  ];

  for (const { bankId, accountId, initialCursor } of bankAccountsToSync) {
    // Construct a unique Workflow ID for each bank account.
    // This allows Temporal to ensure only one active workflow per account.
    const workflowId = `account-statement-sync-${bankId}-${accountId}`;
    console.log(`Attempting to start or get workflow for ${workflowId}`);

    try {
      // Start the workflow.
      // Use client.start() which will throw WorkflowAlreadyStartedError
      // if a workflow with the same ID is already running in the same namespace.
      const handle = await client.start(fetchAndSyncAccountStatements, {
        args: [{ bankId, accountId, initialCursor }], // Pass workflow arguments
        taskQueue: 'account-statement-sync', // Must match the worker's task queue
        workflowId: workflowId, // Unique ID for this workflow instance
        // Optional: Set a workflow run timeout. If it expires, the workflow terminates.
        // workflowRunTimeout: '365 days', // Example: Let it run for a year
      });

      // console.log(`Successfully started workflow ${workflowId} with Run ID: ${handle.runId}`);
      console.log(`Successfully started workflow ${workflowId} with Run ID: `);
    } catch (error) {
      if (error instanceof WorkflowExecutionAlreadyStartedError) {
        console.warn(`Workflow ${workflowId} is already running. Skipping start.`);
        // You could optionally get a handle to the existing workflow here:
        // const existingHandle = client.getHandle(workflowId);
        // And then query or signal it if needed.
      } else {
        console.error(`Failed to start workflow ${workflowId}:`, error);
        // Decide whether to re-throw or continue to the next account.
      }
    }
  }
}

// Execute the client function.
startSyncWorkflows().catch((err) => {
  console.error('Error in client initiation:', err);
  process.exit(1);
});
