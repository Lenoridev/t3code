import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { PersistenceSqlError } from "../persistence/Errors.ts";

/** Delivered changes kept per watch so the agent can still read one an update shortened. */
const DELIVERED_CHANGES_KEPT = 50;

export const CommandWatchHelperFile = Schema.Struct({
  path: Schema.String,
  content: Schema.String,
});
export type CommandWatchHelperFile = typeof CommandWatchHelperFile.Type;
const HelpersJson = Schema.fromJsonString(Schema.Array(CommandWatchHelperFile));
const decodeHelpers = Schema.decodeUnknownSync(HelpersJson);
const encodeHelpers = Schema.encodeSync(HelpersJson);

export type CommandWatchChangeKind = "output.changed" | "initial-state" | "notice";

export interface CommandWatchChange {
  readonly sequence: number;
  readonly kind: CommandWatchChangeKind;
  readonly summary: string;
  readonly content: string;
  readonly detectedAt: string;
}

export interface CommandWatchStorage {
  readonly helpers: ReadonlyArray<CommandWatchHelperFile>;
  /** The last accepted output, null until the first successful check. */
  readonly observation: string | null;
  /** Changes up to here reached the agent, or were cancelled with the update carrying them. */
  readonly deliveredThrough: number;
}

/**
 * Server-side state of command watches: stored helper copies, the last accepted observation, and
 * detected changes. The thread projection only carries the summary clients show.
 */
export class CommandWatchStore extends Context.Service<
  CommandWatchStore,
  {
    readonly create: (input: {
      readonly watchId: string;
      readonly threadId: string;
      readonly helpers: ReadonlyArray<CommandWatchHelperFile>;
      readonly now: string;
    }) => Effect.Effect<void, PersistenceSqlError>;
    readonly get: (
      watchId: string,
    ) => Effect.Effect<CommandWatchStorage | undefined, PersistenceSqlError>;
    /** Saves an accepted observation together with the changes it produced. */
    readonly accept: (input: {
      readonly watchId: string;
      readonly observation: string;
      readonly changes: ReadonlyArray<Omit<CommandWatchChange, "sequence" | "detectedAt">>;
      readonly now: string;
    }) => Effect.Effect<void, PersistenceSqlError>;
    /** Adds a change without replacing the observation, such as a pause notice. */
    readonly note: (input: {
      readonly watchId: string;
      readonly change: Omit<CommandWatchChange, "sequence" | "detectedAt">;
      readonly now: string;
    }) => Effect.Effect<void, PersistenceSqlError>;
    readonly waiting: (
      watchId: string,
    ) => Effect.Effect<ReadonlyArray<CommandWatchChange>, PersistenceSqlError>;
    /** Bytes of change content not yet delivered, notices excluded. */
    readonly waitingBytes: (watchId: string) => Effect.Effect<number, PersistenceSqlError>;
    readonly markDelivered: (
      watchId: string,
      through: number,
    ) => Effect.Effect<void, PersistenceSqlError>;
    readonly readChange: (
      watchId: string,
      sequence: number,
    ) => Effect.Effect<CommandWatchChange | undefined, PersistenceSqlError>;
    /**
     * Moves the changes a replaced watch had not delivered after `deliveredThrough` to its
     * replacement, in order, and removes the replaced watch's storage.
     */
    readonly adopt: (input: {
      readonly fromWatchId: string;
      readonly toWatchId: string;
      readonly deliveredThrough: number;
      readonly now: string;
    }) => Effect.Effect<void, PersistenceSqlError>;
    readonly remove: (watchId: string) => Effect.Effect<void, PersistenceSqlError>;
    readonly watchIds: Effect.Effect<ReadonlyArray<string>, PersistenceSqlError>;
  }
>()("t3/orchestration-v2/CommandWatchStore") {}

interface ChangeRow {
  readonly sequence: number;
  readonly kind: CommandWatchChangeKind;
  readonly summary: string;
  readonly content: string;
  readonly detected_at: string;
}

const toChange = (row: ChangeRow): CommandWatchChange => ({
  sequence: row.sequence,
  kind: row.kind,
  summary: row.summary,
  content: row.content,
  detectedAt: row.detected_at,
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fail = (operation: string) => (cause: unknown) =>
    new PersistenceSqlError({ operation: `CommandWatchStore.${operation}`, cause });

  const insertChanges = Effect.fn("CommandWatchStore.insertChanges")(function* (
    watchId: string,
    changes: ReadonlyArray<
      Omit<CommandWatchChange, "sequence" | "detectedAt"> & { readonly detectedAt?: string }
    >,
    now: string,
  ) {
    const [row] = yield* sql<{ readonly next_sequence: number }>`
        SELECT next_sequence FROM command_watch_storage WHERE watch_id = ${watchId}
      `;
    if (row === undefined) return false;
    let sequence = row.next_sequence;
    for (const change of changes) {
      yield* sql`
          INSERT INTO command_watch_changes (watch_id, sequence, kind, summary, content, detected_at)
          VALUES (${watchId}, ${sequence}, ${change.kind}, ${change.summary}, ${change.content}, ${change.detectedAt ?? now})
        `;
      sequence += 1;
    }
    yield* sql`
        UPDATE command_watch_storage SET next_sequence = ${sequence}, updated_at = ${now}
        WHERE watch_id = ${watchId}
      `;
    return true;
  });

  return CommandWatchStore.of({
    create: Effect.fn("CommandWatchStore.create")(function* ({ watchId, threadId, helpers, now }) {
      return yield* sql`
        INSERT INTO command_watch_storage
          (watch_id, thread_id, helpers_json, observation, next_sequence, delivered_through, updated_at)
        VALUES (${watchId}, ${threadId}, ${encodeHelpers(helpers)}, NULL, 1, 0, ${now})
        ON CONFLICT (watch_id) DO NOTHING
      `.pipe(Effect.asVoid, Effect.mapError(fail("create")));
    }),

    get: Effect.fn("CommandWatchStore.get")(function* (watchId) {
      return yield* sql<{
        readonly helpers_json: string;
        readonly observation: string | null;
        readonly delivered_through: number;
      }>`
        SELECT helpers_json, observation, delivered_through
        FROM command_watch_storage WHERE watch_id = ${watchId}
      `.pipe(
        Effect.map(([row]) =>
          row === undefined
            ? undefined
            : {
                helpers: decodeHelpers(row.helpers_json),
                observation: row.observation,
                deliveredThrough: row.delivered_through,
              },
        ),
        Effect.mapError(fail("get")),
      );
    }),

    accept: Effect.fn("CommandWatchStore.accept")(function* ({
      watchId,
      observation,
      changes,
      now,
    }) {
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            if (!(yield* insertChanges(watchId, changes, now))) return;
            yield* sql`
              UPDATE command_watch_storage SET observation = ${observation}, updated_at = ${now}
              WHERE watch_id = ${watchId}
            `;
          }),
        )
        .pipe(Effect.mapError(fail("accept")));
    }),

    note: Effect.fn("CommandWatchStore.note")(function* ({ watchId, change, now }) {
      return yield* sql
        .withTransaction(insertChanges(watchId, [change], now))
        .pipe(Effect.asVoid, Effect.mapError(fail("note")));
    }),

    waiting: Effect.fn("CommandWatchStore.waiting")(function* (watchId) {
      return yield* sql<ChangeRow>`
        SELECT c.sequence, c.kind, c.summary, c.content, c.detected_at
        FROM command_watch_changes c
        JOIN command_watch_storage s ON s.watch_id = c.watch_id
        WHERE c.watch_id = ${watchId} AND c.sequence > s.delivered_through
        ORDER BY c.sequence ASC
      `.pipe(
        Effect.map((rows) => rows.map(toChange)),
        Effect.mapError(fail("waiting")),
      );
    }),

    waitingBytes: Effect.fn("CommandWatchStore.waitingBytes")(function* (watchId) {
      return yield* sql<{ readonly bytes: number | null }>`
        SELECT SUM(LENGTH(CAST(c.content AS BLOB))) AS bytes
        FROM command_watch_changes c
        JOIN command_watch_storage s ON s.watch_id = c.watch_id
        WHERE c.watch_id = ${watchId} AND c.sequence > s.delivered_through AND c.kind != 'notice'
      `.pipe(
        Effect.map(([row]) => row?.bytes ?? 0),
        Effect.mapError(fail("waitingBytes")),
      );
    }),

    markDelivered: Effect.fn("CommandWatchStore.markDelivered")(function* (watchId, through) {
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`
              UPDATE command_watch_storage
              SET delivered_through = MAX(delivered_through, ${through})
              WHERE watch_id = ${watchId}
            `;
            yield* sql`
              DELETE FROM command_watch_changes
              WHERE watch_id = ${watchId}
                AND sequence <= ${through - DELIVERED_CHANGES_KEPT}
            `;
          }),
        )
        .pipe(Effect.mapError(fail("markDelivered")));
    }),

    readChange: Effect.fn("CommandWatchStore.readChange")(function* (watchId, sequence) {
      return yield* sql<ChangeRow>`
        SELECT sequence, kind, summary, content, detected_at
        FROM command_watch_changes WHERE watch_id = ${watchId} AND sequence = ${sequence}
      `.pipe(
        Effect.map(([row]) => (row === undefined ? undefined : toChange(row))),
        Effect.mapError(fail("readChange")),
      );
    }),

    adopt: Effect.fn("CommandWatchStore.adopt")(function* ({
      fromWatchId,
      toWatchId,
      deliveredThrough,
      now,
    }) {
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const rows = yield* sql<ChangeRow>`
              SELECT c.sequence, c.kind, c.summary, c.content, c.detected_at
              FROM command_watch_changes c
              JOIN command_watch_storage s ON s.watch_id = c.watch_id
              WHERE c.watch_id = ${fromWatchId}
                AND c.sequence > MAX(s.delivered_through, ${deliveredThrough})
              ORDER BY c.sequence ASC
            `;
            yield* insertChanges(toWatchId, rows.map(toChange), now);
            yield* sql`DELETE FROM command_watch_changes WHERE watch_id = ${fromWatchId}`;
            yield* sql`DELETE FROM command_watch_storage WHERE watch_id = ${fromWatchId}`;
          }),
        )
        .pipe(Effect.mapError(fail("adopt")));
    }),

    remove: Effect.fn("CommandWatchStore.remove")(function* (watchId) {
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`DELETE FROM command_watch_changes WHERE watch_id = ${watchId}`;
            yield* sql`DELETE FROM command_watch_storage WHERE watch_id = ${watchId}`;
          }),
        )
        .pipe(Effect.mapError(fail("remove")));
    }),

    watchIds: sql<{ readonly watch_id: string }>`
      SELECT watch_id FROM command_watch_storage
    `.pipe(
      Effect.map((rows) => rows.map((row) => row.watch_id)),
      Effect.mapError(fail("watchIds")),
    ),
  });
});

export const layer = Layer.effect(CommandWatchStore, make);
