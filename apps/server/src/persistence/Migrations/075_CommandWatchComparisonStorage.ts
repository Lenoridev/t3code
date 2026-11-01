import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`ALTER TABLE command_watch_storage ADD COLUMN comparison_source TEXT`;
  yield* sql`ALTER TABLE command_watch_storage ADD COLUMN comparison_state TEXT`;
  yield* sql`ALTER TABLE command_watch_storage ADD COLUMN completion_reason TEXT`;
  yield* sql`ALTER TABLE command_watch_storage ADD COLUMN next_check_at INTEGER`;
});
