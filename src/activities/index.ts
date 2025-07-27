import axios from 'axios';
import { ApplicationFailure } from '@temporalio/common';

// In a real production application, these would be loaded from environment variables
// or a secure configuration service.
const BANK_API_BASE_URL = process.env.BANK_API_BASE_URL || 'https://bank.example.com/api';
const DESTINATION_API_BASE_URL = process.env.DESTINATION_API_BASE_URL || 'https://destination.example.com/api';
const BANK_API_KEY_PREFIX = 'YOUR_BANK_API_KEY_'; // Example for specific bank keys
const DESTINATION_API_KEY = 'YOUR_DESTINATION_API_KEY';

/**
 * Custom Error for Authentication Failures.
 * Marked as non-retryable to prevent endless retries on bad credentials.
 */
class BankAuthError extends ApplicationFailure {
  constructor(message: string, details?: any[]) {
    super(message, 'BankAuthError', true, details); // nonRetryable = true
  }
}

class DestinationAuthError extends ApplicationFailure {
  constructor(message: string, details?: any[]) {
    super(message, 'DestinationAuthError', true, details); // nonRetryable = true
  }
}

class InvalidInputError extends ApplicationFailure {
  constructor(message: string, details?: any[]) {
    super(message, 'InvalidInputError', true, details); // nonRetryable = true
  }
}

/**
 * Fetches a single page of bank statements from the source API.
 * This activity handles API calls, error translation, and pagination details.
 */
export async function fetchBankStatementsPage(
  bankId: string,
  accountId: string,
  cursor?: string
): Promise<{ data: any[]; nextCursor?: string }> {
  try {
    const params: any = { limit: 100 }; // Configurable limit for the bank API call
    if (cursor) {
      params.cursor = cursor; // Use cursor for pagination
    }

    // Example: Dynamically get bank-specific API key
    const bankApiKey = process.env[`${BANK_API_KEY_PREFIX}${bankId.toUpperCase()}`] || 'default-bank-key';

    const response = await axios.get(`${BANK_API_BASE_URL}/banks/${bankId}/accounts/${accountId}/statements`, {
      params,
      headers: {
        'Authorization': `Bearer ${bankApiKey}`, // Include authentication header
        'Accept': 'application/json',
      },
      timeout: 30000, // 30 seconds API call timeout
    });

    // Assuming the bank API returns a structure like: { data: [...statements], next_cursor: "..." }
    const statements = response.data.data || [];
    const nextCursor = response.data.next_cursor || response.data.pagination?.next_token; // Adapt to actual API

    console.log(`Activity: Fetched ${statements.length} statements for ${bankId}-${accountId}. Next cursor: ${nextCursor || 'N/A'}`);
    return { data: statements, nextCursor };

  } catch (error: any) {
    console.error(`Activity Error: Failed to fetch bank statements for ${bankId}-${accountId}:`, error.message);
    if (axios.isAxiosError(error)) {
      if (error.response) {
        // Handle specific HTTP status codes
        if (error.response.status === 429) {
          // Rate limit: This specific error should be handled by the workflow for longer waits
          throw ApplicationFailure.create({
            message: 'Rate Limit Exceeded',
            details: [`Bank API rate limit hit for ${bankId}. Response: ${JSON.stringify(error.response.data)}`],
          });
        }
        if (error.response.status === 401 || error.response.status === 403) {
          // Authentication/Authorization error: Not retryable, requires human intervention
          throw new BankAuthError('Authentication or authorization failed with bank API', [
            `Status: ${error.response.status}`,
            `Data: ${JSON.stringify(error.response.data)}`,
          ]);
        }
        if (error.response.status >= 400 && error.response.status < 500) {
          // Client-side errors (e.g., bad request, not found) might be non-retryable
          // Depending on the context, you might make this non-retryable or retryable based on error content
          if (error.response.status === 400) {
              throw new InvalidInputError(`Invalid input to Bank API: ${error.response.status}`, [JSON.stringify(error.response.data)]);
          }
          // Default to retryable for other client errors, or make specific
          throw ApplicationFailure.create({
            message: `Bank API Client Error: ${error.response.status}`,
            details: [`Data: ${JSON.stringify(error.response.data)}`],
          });
        }
        if (error.response.status >= 500) {
          // Server-side errors: Generally retryable
          throw ApplicationFailure.create({
            message: `Bank API Server Error: ${error.response.status}`,
            details: [`Data: ${JSON.stringify(error.response.data)}`],
          });
        }
      } else if (error.request) {
        // Network error (no response received)
        throw ApplicationFailure.create({
          message: 'Network Error while connecting to Bank API',
          details: [error.message],
        });
      }
    }
    // Catch-all for any other unhandled errors
    throw ApplicationFailure.create({
      message: 'Unknown error fetching bank statements',
      details: [error.message],
    });
  }
}

/**
 * Syncs a batch of statements to the destination API.
 * This activity should ideally be idempotent on the destination side.
 */
export async function syncStatementsToDestination(
  bankId: string,
  accountId: string,
  statements: any[]
): Promise<void> {
  if (statements.length === 0) {
    console.log('Activity: No statements to sync to destination.');
    return;
  }

  try {
    const response = await axios.post(`${DESTINATION_API_BASE_URL}/sync/statements`, {
      bankId,
      accountId,
      statements,
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${DESTINATION_API_KEY}`, // Include authentication header
      },
      timeout: 60000, // 60 seconds API call timeout
    });

    console.log(`Activity: Successfully synced ${statements.length} statements for ${bankId}-${accountId} to destination.`);
  } catch (error: any) {
    console.error(`Activity Error: Failed to sync statements to destination for ${bankId}-${accountId}:`, error.message);
    if (axios.isAxiosError(error)) {
      if (error.response) {
        if (error.response.status === 401 || error.response.status === 403) {
          throw new DestinationAuthError('Authentication or authorization failed with destination API', [
            `Status: ${error.response.status}`,
            `Data: ${JSON.stringify(error.response.data)}`,
          ]);
        }
        if (error.response.status >= 400 && error.response.status < 500) {
          throw ApplicationFailure.create({
            message: `Destination API Client Error: ${error.response.status}`,
            details: [`Data: ${JSON.stringify(error.response.data)}`],
          });
        }
        if (error.response.status >= 500) {
          throw ApplicationFailure.create({
            message: `Destination API Server Error: ${error.response.status}`,
            details: [`Data: ${JSON.stringify(error.response.data)}`],
          });
        }
      } else if (error.request) {
        throw ApplicationFailure.create({
          message: 'Network Error while connecting to Destination API',
          details: [error.message],
        });
      }
    }
    throw ApplicationFailure.create({
      message: 'Unknown error syncing statements to destination',
      details: [error.message],
    });
  }
}

/**
 * Fetches the last successfully synced pagination cursor for a given bankId and accountId
 * from a persistent store (e.g., PostgreSQL, MongoDB, Redis).
 * This is crucial for resuming workflows from the last known good state.
 */
export async function getLastSyncedCursor(bankId: string, accountId: string): Promise<string | undefined> {
  console.log(`Activity: Retrieving last synced cursor for ${bankId}-${accountId} from database.`);
  // --- Replace with actual database interaction ---
  // Example: Query your database:
  // const result = await db.query('SELECT cursor FROM sync_metadata WHERE bank_id = $1 AND account_id = $2', [bankId, accountId]);
  // return result.rows[0]?.cursor;
  // For this example, we'll just return undefined to simulate a fresh start.
  // In a real app, you might have a dedicated table like `sync_cursors` with `bank_id`, `account_id`, `last_cursor`, `last_synced_at`.
  return undefined;
}

/**
 * Updates the last successfully synced pagination cursor for a given bankId and accountId
 * in a persistent store.
 */
export async function updateLastSyncedCursor(bankId: string, accountId: string, cursor: string): Promise<void> {
  console.log(`Activity: Updating last synced cursor for ${bankId}-${accountId} to: ${cursor} in database.`);
  // --- Replace with actual database interaction ---
  // Example: Upsert into your database:
  // await db.query(
  //   'INSERT INTO sync_metadata (bank_id, account_id, cursor, last_synced_at) VALUES ($1, $2, $3, NOW()) ON CONFLICT (bank_id, account_id) DO UPDATE SET cursor = EXCLUDED.cursor, last_synced_at = EXCLUDED.last_synced_at;',
  //   [bankId, accountId, cursor]
  // );
}

/**
 * Activity to detect changed data.
 * For account statements, "changed data" usually means new entries. The pagination loop handles this.
 * If you need to detect updates/deletions to *existing* statements, this activity would
 * involve more sophisticated logic:
 * 1. Fetching the "current state" of statements for a given `bankId` and `accountId` from *your* destination.
 * 2. Comparing the `pageData` (newly fetched statements) against your current state.
 * 3. Identifying true updates, inserts, or deletes.
 * This is more complex and depends heavily on how bank APIs expose changes to existing records.
 * Many statement APIs only provide new entries.
 */
export async function detectChangedData(
  bankId: string,
  accountId: string,
  newStatements: any[] // This would be the latest batch of statements
): Promise<{ newInserts: any[]; updatedRecords: any[]; deletedRecords: any[]; hasChanges: boolean }> {
  console.log(`Activity: Performing change detection for ${bankId}-${accountId} (conceptual).`);
  // Placeholder implementation:
  // In a real scenario, you might:
  // a) Fetch existing statements from your application's database.
  // b) Implement a diffing algorithm (e.g., comparing by transaction ID and a content hash).
  // For statement APIs, often just new inserts are handled by the pagination.
  return { newInserts: newStatements, updatedRecords: [], deletedRecords: [], hasChanges: newStatements.length > 0 };
}
