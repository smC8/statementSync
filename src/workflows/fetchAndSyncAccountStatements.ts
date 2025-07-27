import { proxyActivities, sleep, setHandler, defineSignal } from '@temporalio/workflow'; // Added defineSignal
import { ApplicationFailure } from '@temporalio/common';
import type * as activities from '../activities'; // Import all activities from your activities file
import ms from 'ms';

// Define the signal for updating the cursor
const updateCursorSignal = defineSignal<[string]>('updateCursor');

// Proxy activities to make them callable from the workflow.
// Configure common retry policies here. Specific retries can be overridden in activities.
const { fetchBankStatementsPage, syncStatementsToDestination, getLastSyncedCursor, updateLastSyncedCursor } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes', // Max time an Activity can run. Adjust based on expected API response times.
  retry: {
    initialInterval: '10 seconds', // Start with a short retry interval
    backoffCoefficient: 2,         // Exponential backoff
    maximumInterval: '2 minutes',  // Cap the retry interval
    maximumAttempts: 5,            // Maximum number of retries for transient errors
    nonRetryableErrorTypes: ['BankAuthError', 'DestinationAuthError', 'InvalidInputError'], // Custom error types for unrecoverable issues
  },
});

/**
 * Input parameters for the fetch and sync workflow.
 */
interface FetchAndSyncInput {
  bankId: string;
  accountId: string;
  initialCursor?: string; // Optional: Allows starting a sync from a specific point
  syncBatchSize?: number; // Number of entries to sync in a single destination API call
  pollingInterval?: string | any; // How often the workflow should check for new data (e.g., '1 hour')
}

/**
 * Main workflow function to fetch and sync account statements.
 * This workflow is designed to run indefinitely for continuous synchronization.
 */
export async function fetchAndSyncAccountStatements({
  bankId,
  accountId,
  initialCursor,
  syncBatchSize = 100, // Default batch size for syncing to destination
  pollingInterval = '1 hour', // Default polling interval
}: FetchAndSyncInput): Promise<void> {
  let currentCursor = initialCursor;
  let hasMoreDataInCurrentFetch = true;

  // --- Signal Handler for External Control ---
  // This allows external systems to update the cursor, for example,
  // to force a re-sync from an earlier point or reset.
  setHandler(updateCursorSignal, (newCursor: string) => { // Corrected: Using updateCursorSignal definition
    currentCursor = newCursor;
    hasMoreDataInCurrentFetch = true; // Force a new fetch from the updated cursor
    console.log(`Workflow for ${bankId}-${accountId} received signal to update cursor to: ${newCursor}`);
  });

  // Main loop for continuous data fetching and syncing
  while (true) { // Loop indefinitely for continuous sync
    let pageData: any[] = [];
    let nextPageCursor: string | undefined;

    // 1. Fetch the last successfully synced cursor from a durable store (e.g., database)
    // This is crucial for resuming correctly after workflow pauses or worker restarts.
    if (!currentCursor) { // Only fetch from DB if no initial cursor was provided or explicitly reset
        currentCursor = await getLastSyncedCursor(bankId, accountId);
        console.log(`Loaded initial cursor for ${bankId}-${accountId} from DB: ${currentCursor || 'None'}`);
    }

    // 2. Loop to fetch all available paginated data from the bank API
    do {
      try {
        console.log(`Fetching bank statements for ${bankId}-${accountId} from cursor: ${currentCursor || 'Start'}`);
        const fetchResult = await fetchBankStatementsPage(bankId, accountId, currentCursor);
        pageData = fetchResult.data;
        nextPageCursor = fetchResult.nextCursor;

        if (pageData.length > 0) {
          // 3. Sync fetched data to the Destination API
          console.log(`Syncing ${pageData.length} statements for ${bankId}-${accountId} to destination.`);
          await syncStatementsToDestination(bankId, accountId, pageData);

          // Optional: If 'detect changed data' is a separate logical step,
          // you could call an activity here, e.g.,
          // await detectChangedData(bankId, accountId, pageData);
        }

        // 4. Update the last synced cursor in a durable store
        if (nextPageCursor) {
          await updateLastSyncedCursor(bankId, accountId, nextPageCursor);
          currentCursor = nextPageCursor; // Move to the next cursor for the next iteration
          hasMoreDataInCurrentFetch = true; // Indicate there might be more pages
        } else {
          hasMoreDataInCurrentFetch = false; // No more pages from this bank API call
        }

      } catch (error: any) {
        console.error(`Workflow error during fetch/sync for ${bankId}-${accountId}:`, error.message);
        // Handle specific application failures to implement custom logic (e.g., waiting for rate limit)
        if (error instanceof ApplicationFailure) {
          if (error.message.includes('Rate Limit Exceeded')) {
            console.warn(`Bank API rate limit hit for ${bankId}. Waiting before retrying...`);
            await sleep('5 minutes'); // Wait a longer period before retrying the same page
            hasMoreDataInCurrentFetch = true; // Still trying to get the current page
            continue; // Re-attempt fetching the current page with the same cursor
          }
          // For non-retryable errors (e.g., auth errors), throw to terminate or signal
          if (error.nonRetryable) {
            console.error(`Non-retryable error for ${bankId}-${accountId}. Workflow will terminate.`);
            throw error; // Re-throw to allow Temporal to record as failed workflow
          }
        }
        // For other errors, let Temporal's activity retry mechanism handle it.
        // If retries are exhausted, the workflow will eventually fail.
        throw error;
      }
    } while (hasMoreDataInCurrentFetch && nextPageCursor); // Continue fetching pages as long as there's a next cursor

    // 5. After fetching all available pages, pause for the polling interval
    // and then re-evaluate for new data.
    console.log(`Finished fetching all available data for ${bankId}-${accountId}. Waiting for ${pollingInterval} for next cycle.`);
    // await sleep((pollingInterval)); // Corrected: pollingInterval is a valid DurationInput string
    const intervalMs = ms(pollingInterval);
    if (intervalMs === undefined) {  
      throw new Error(`Invalid polling interval: ${pollingInterval}`);
    }
    await sleep(intervalMs); // ✅ correct Temporal Duration


    // await sleep((3600)); // Corrected: pollingInterval is a valid DurationInput string
    
    // Reset currentCursor to fetch from the latest known synced point in the DB
    // This handles scenarios where the workflow pauses and new data arrived in the interim.
    currentCursor = undefined; // Force lookup from DB on next loop iteration
  }
}
