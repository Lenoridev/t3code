import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Kept out of the thread projection so observations and waiting changes never reach clients.
  yield* sql`
    CREATE TABLE command_watch_storage (
      watch_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      helpers_json TEXT NOT NULL,
      observation TEXT,
      next_sequence INTEGER NOT NULL,
      delivered_through INTEGER NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;

  yield* sql`
    CREATE TABLE command_watch_changes (
      watch_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      kind TEXT NOT NULL,
      summary TEXT NOT NULL,
      content TEXT NOT NULL,
      detected_at TEXT NOT NULL,
      PRIMARY KEY (watch_id, sequence)
    )
  `;
});
