# Change Log
All noteworthy changes to this plugin will be documented in this file.

## Unreleased
### Changed
- Consumer and producer record tables now retain up to 50,000 records per session. Previously, leaving the "Consumer records limit" field empty (or set to 0) allowed unbounded growth.
- Schema updates now time out after 3 minutes instead of waiting indefinitely; Cancel stops waiting for a pending update (the registry may still apply it), and the limit can be changed with the Registry key `bdide.kafka.schema.registry.call.timeout.ms`.
- Confluent Cloud requests are now retried with exponential backoff and jitter, honoring `Retry-After`: on rate limiting (429) for any request, and on temporary server errors (502/503/504) and connection failures for read requests. If the server asks to wait more than a minute, the request fails right away instead of retrying sooner than asked.
- Tool window auto-refresh now varies each interval by up to ±20%. While a connection is unreachable, auto-refresh keeps re-checking it, waiting progressively longer (up to about 5 minutes), and resumes the normal interval once the connection recovers. Connections that use an SSH tunnel are not re-checked automatically; use Refresh to reconnect them.
- Upgrade Apache Kafka client to 4.1.2 (required by the Schema Registry client's OAuth token provider)
- The schema list of the consumer and producer editors no longer looks up the format of every schema of the registry. That took minutes with thousands of schemas, and a request for each of them was sent again every time an editor was opened. The formats of the selected schema and of the schema of the topic are looked up first, then those of the schemas that the list shows on screen. Schemas whose format can not be found, for example subjects that you may not read, are not asked for again for a minute
- The schema names are kept for a minute. The editors show the names of the last listing right away while the Schema Registry is asked for the current ones, which takes seconds on a big registry. The list is corrected when the Schema Registry answers with other names. Creating or deleting schemas lists the names again
### Added
- MessagePack serialization support for consumer and producer records
- OAuth 2.0 (client credentials) authentication for Confluent Schema Registry connections
- Loading topics, schema names or schema details that takes a second or longer is now logged at INFO level in `idea.log`, which helps to find out why a connection feels slow
- The details of a consumed record show the schema the record was written with: **Key schema** and **Value schema** with the ID of the schema and the subjects and versions it is registered as, like `100016 (orders-value, version 3)`. A schema that is registered under many subjects shows the subjects of the topic first and counts the others. Records that name a schema by its GUID show the GUID. The Schema Registry is asked once for each ID, which is remembered for a few minutes, and the details of the record do not wait for it. For Confluent Schema Registries including Confluent Cloud
### Removed
### Fixed
- Deleting a consumer group that the broker rejects (for example, one that still has active members) now shows an error instead of appearing to succeed.
- The Topics table no longer shows "Create a topic" mixed into the connection error message when an auto-refresh fails.
- Producer send timeouts now show as error rows in the results table instead of silently ending the batch. Auto mode stops after 3 consecutive timeouts, and timed-out sends no longer count toward the request total.
- When producing from a CSV file in auto mode, a row whose send times out after reaching the producer's queue (for example, its batch expires after `delivery.timeout.ms`) is no longer sent again, since it may already have been written. Only rows that never left the client are retried.
- The producer and consumer status label now shows an error icon when an error occurs and keeps it after stopping. An error that happens before any record arrives no longer changes to "Canceled" when stopped.
- The Confluent Cloud consumer no longer retries server errors forever, or keeps polling after you sign out. It now retries network errors, timeouts, rate limiting and server errors, and stops with an error after 10 consecutive failed polls (about 2 minutes for most errors).
- Confluent Cloud sessions whose refresh token is rejected now sign out right away instead of retrying for several minutes. Temporary token refresh failures back off between tries and sign out after 10 failed attempts.
- Producer and consumer connections now report their own client IDs (`… - Producer`, `… - Consumer`) to the broker instead of the admin client's `… - Admin` ID.
- The Confluent Cloud producer still retries produce requests on temporary server errors (502/503/504), and now shows an error row and stops when those retries run out, or when the server asks to wait more than a minute, instead of only showing a notification. It no longer repeats rate-limited (429) requests that were already retried.
- Restarting the Confluent Cloud producer right after stopping it no longer lets the stopped run, still finishing its last produce request, stop the new run. While a producer is stopping, its button shows "Stopping..." and is disabled until that request finishes, so a second click no longer starts a new run.
- Stopping the Confluent Cloud producer while it is waiting out rate limiting (429) now takes effect right away, instead of after the wait (up to about 3 minutes). A produce request already sent still finishes, so its record is reported.
- Confluent Cloud tokens now refresh well before they expire, at a random point between 70% and 90% of the lifetime stated in each token, instead of a few seconds before. A slow refresh no longer leaves requests going out with an expired token.
- Faster and more reliable schema detection and selection in the consumer and producer editors on registries with many subjects: the schema list shows up right after the schema names are listed and the formats follow, starting with the schema of the topic; schema formats are loaded a few at a time and remembered instead of one after another or all at once, requests for the same schema are shared with the tool window, the schema of a topic is detected without loading every subject, and a saved schema is selected by its name and stays selected when the editor restores its topic
- Confluent Cloud: schemas whose details were not loaded yet were shown, and offered in the editors, as Avro
- A schema that was chosen in the consumer or producer editor, or saved with it, but no longer exists in the Schema Registry is no longer replaced by the first schema of the list without notice. The schema field is left empty and says that it must not be empty
- The lines in `idea.log` about listing the schemas for the consumer and producer editors were logged under the category `kotlinx.coroutines.CoroutineScope`. They are now logged under the category of the schema list and tell whether they are about the key or the value
- Cancelling a load of schemas or topics, for example because the connection was refreshed or closed while it ran, was handled like a failed load: it was logged as a warning and could be shown as an error. It now just ends

## 253.25910.2
### Fixed
- Plugin failed to load with a `NoClassDefFoundError` in IDEs without the Remote/SSH module, such as WebStorm (https://github.com/confluentinc/intellij/issues/628)
- Key & Value fields in the Consumer details panel stayed cramped for large values on tall displays (https://github.com/confluentinc/intellij/issues/588)

## 253.25910.1
### Changed
- Adjust error reporting filter to remove non-Confluent errors

### Fixed
- Value & Key fields in Consumer details panel too small to read (https://github.com/confluentinc/intellij/issues/601)
- Viewing schemas that reference other schemas throws an error (https://github.com/confluentinc/intellij/issues/591)

## 253.25910.0
### Changed
- Rebrand plugin from "Kafka" to "Confluent" and publish under Apache 2 license

### Added
- OAuth support for logging in to Confluent Cloud directly, with the ability to view & interact with environments and resources.

### Removed

### Fixed
- Consumer performance improvements: batch table updates and optimized storage