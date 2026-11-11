# Handoff: OAuth for Schema Registry and performance work, split into pull requests

State of 2026-11-11. This file exists only on the branch `handoff/pr-split`. That branch is the complete result
(identical to `backup/all-optimizations`) plus this note and the pointers to it in `AGENTS.md`, `CLAUDE.md` and
`.github/copilot-instructions.md`. **Never open a pull request from it, never merge it, and never base new work on it.**

## In one minute

- The fork `mwiltzer/intellij` (public, forked from `confluentinc/intellij`) adds OAuth 2.0 client credentials for
  Confluent Schema Registry (branch `oauth-4-schem-reg`) and about twenty performance and usability improvements. Most
  of them are for Schema Registries with more than 10,000 subjects, where listing all subjects takes 4–5 seconds.
- The improvements were developed as one bundle, which is kept as `backup/all-optimizations`. That bundle was split
  into **21 branches with one commit each** on top of `oauth-4-schem-reg`, so that each improvement can be its own pull
  request. All branches are pushed to `origin`. No pull request exists yet.
- Every branch passes the full test suite on its own. Merging all of them gives exactly the backup, apart from the two
  intended differences listed below; that merge passes all 2,666 tests and `buildPlugin`.
- **Decided on 2026-11-11: upstream first, the fork as fallback.** The pull requests go to `confluentinc/intellij`;
  whatever upstream does not take lives on in the fork. **Next step:** step 1 of the upstream plan below.
- What changed for users is listed in `CHANGELOG.md` (section Unreleased) and documented in
  `docs/kafka-plugin-manual.md` on this branch.

## Working agreements from the session

- Ask before pushing, force-pushing, opening pull requests or merging.
- Never commit `.java-version`. It is a local jenv file and stays untracked.
- Before implementing larger ideas that were not approved, present them rated by value, effort and risk; ask about
  design decisions with concrete choices. Keep answers short.
- Commit messages follow Conventional Commits and explain why. Commits made by GitHub Copilot carry the trailer
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`.
- Project conventions: `CLAUDE.md`, `.github/copilot-instructions.md`, `AI_POLICY.md` (follow existing patterns,
  comments say why rather than what, unit tests with JUnit 5, `@TestApplication`, backtick names, `@Nested`, fixtures in
  `test/resources`). User-visible changes get a `CHANGELOG.md` entry under Unreleased and, where useful, a paragraph in
  the manual.

## Branches

```
main
└── oauth-4-schem-reg                                  O
    ├── L1 L3 L4 L5 L6 L7 L8 L11 L12 L13               independent, one commit each
    ├── L2 ── L9
    └── S0 ── S1 ── S2 ── S3 ── S4 ── S5 ── S6         stack, one commit each
              ├── L10
              └── L14
```

| ID  | Branch                                 | Based on | Commit     | What it does                                                                                                                                                                                                                                                  | Tests        |
|-----|----------------------------------------|----------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| O   | `oauth-4-schem-reg`                    | `main`   | `85e7d86a` | 4 commits: the user's OAuth commit `c3281288` (includes Kafka clients 4.0.0 → 4.1.2), `6e65e118` Kafka config classes built with the plugin class loader, `3ee104b3` one token request for concurrent calls, `85e7d86a` OAuth fields checked by Test Connection | green        |
| L1  | `chore/dependency-bumps`               | O        | `a315998f` | moshi 1.15.2, JUnit 5.14.4 / platform 1.14.4, mockito-kotlin 6.4.0, kotlin-metadata 2.2.21, Sentry Gradle plugin 5.12.2, compiler flag `-Xannotation-default-target=first-only`                                                                                | green        |
| L2  | `fix/avro-fixed-decimal`               | O        | `cd1b92e4` | Random data for an Avro decimal stored as `fixed` failed                                                                                                                                                                                                      | green        |
| L3  | `fix/ccloud-cache-concurrency`         | O        | `66b781de` | Confluent Cloud caches safe for concurrent use                                                                                                                                                                                                                | green        |
| L4  | `perf/virtual-topic-detection`         | O        | `48345adc` | Virtual topics are detected once per topic; a topic the user may not describe no longer breaks the detection                                                                                                                                                  | green        |
| L5  | `fix/install-notification-per-project` | O        | `377dfb09` | One "plugin installed" notification per open project (the shared one caused a SEVERE "BalloonImpl already disposed")                                                                                                                                         | green        |
| L6  | `fix/bdt-url-handler-classcast`        | O        | `cd5fb4b3` | No `ClassCastException` stack traces in `idea.log` when Big Data Tools is installed                                                                                                                                                                           | green        |
| L7  | `perf/record-table-memory`             | O        | `74569439` | Record tables keep only the text of keys and values (about 5× less heap)                                                                                                                                                                                      | green        |
| L8  | `perf/consumer-filter`                 | O        | `02fb84ad` | Consumer filter: regex compiled once, checked while typing, matched against the text shown                                                                                                                                                                    | green        |
| L9  | `feat/avro-json-replay`                | L2       | `f6a56fc9` | Produce Avro values from JSON as tools like AKHQ show it, to replay records                                                                                                                                                                                   | green        |
| L11 | `feat/producer-schema-version`         | O        | `e8f2f296` | The producer can write with an older version of the registry schema                                                                                                                                                                                           | 1,815        |
| L12 | `fix/registry-error-hints`             | O        | `629868fe` | Says why the Schema Registry is not available (untrusted TLS certificate, rejected OAuth token, …); manual: a custom truststore replaces the default one                                                                                                       | 1,783        |
| L13 | `perf/registry-connection-check`       | O        | `56b5132a` | The connection check asks the registry for one subject instead of all                                                                                                                                                                                         | 1,773        |
| S0  | `perf/log-slow-loads`                  | O        | `3cbea0cc` | Slow topic loads (≥ 1 s) logged at INFO; a cancelled load ends quietly instead of being logged and shown as an error                                                                                                                                          | 1,781        |
| S1  | `perf/editor-schema-loading`           | S0       | `1d3a6fbb` | Editors: one shared listing of the schema names (kept a minute, older names shown while refreshing), formats looked up on demand and a few at a time, saved schema kept by name, CCloud names-only entries no longer taken for Avro, SR client cache 1000 | 1,913        |
| S2  | `perf/schema-registry-section`         | S1       | `7c7ba1d2` | The Schema Registry section is shown from one listing; filter, limit and favorites re-slice it; the filter is a case-insensitive "contains" instead of a regex                                                                                                | 1,986        |
| S3  | `feat/hide-internal-and-other-schemas` | S2       | `206c1fd8` | Hides the schemas of internal topics and of topics the user can not see, with two switches (default off)                                                                                                                                                     | 2,066        |
| S4  | `feat/editor-schema-order`             | S3       | `9343f428` | The editors list the schemas that matter first (the topic's schemas, favorites, then the rest, internal ones last)                                                                                                                                            | 2,094        |
| S5  | `feat/schema-prefix-filter`            | S4       | `7dee2fb6` | `^prefix` filter in the Schema Registry section, served by `GET /subjects?subjectPrefix=`                                                                                                                                                                      | 2,218        |
| S6  | `perf/editor-topic-schema-lookup`      | S5       | `2d286ea3` | Editors find the topic's schema by prefix without waiting for the full listing; names kept 5 minutes where prefix listing works; logs of the UI-thread wait and of the selection reason                                                                       | 2,289        |
| L10 | `fix/schema-combo-width`               | S1       | `8d45d0f8` | Long schema names no longer push the buttons next to the schema list out of view                                                                                                                                                                              | 1,915        |
| L14 | `feat/record-schema-id`                | S1       | `a568128b` | The details of a consumed record show the schema ID and the subjects and versions it is registered as (called C3 in the session notes)                                                                                                                        | 2,033        |
| –   | `backup/all-optimizations`             | O        | `a13e3eff` | The whole bundle as one WIP commit: the target state and the reference for resolving conflicts. Never merge it.                                                                                                                                              | –            |
| –   | `handoff/pr-split`                     | backup   | –          | The backup plus this note. Never merge it.                                                                                                                                                                                                                   | –            |

All of them merged together: **2,666 tests, 0 failures, 2 known skips, `buildPlugin` OK.** The commit message of each
branch is written to serve as the description of its pull request. The commit IDs are those before the rebase onto
upstream (step 2 of the upstream plan). Once `integration` exists, it replaces `handoff/pr-split` as the branch with
everything.

## Upstream plan (decided 2026-11-11)

The user wants the work in the original repository, `confluentinc/intellij`. Whatever the maintainers do not want stays
in the fork. The fork is set up so that this needs no switch later:

- The fork's `main` stays a plain mirror of upstream `main`: only fast-forwarded, never committed to.
- A branch `integration` (still to be created) is upstream `main` plus all branches, merged in the order of the next
  section. It is the plugin the user builds and installs, from the first day on. Rebuild it whenever upstream `main`
  or a branch changes; a branch that upstream merged no longer adds anything to it.

### Facts on 2026-11-11

- Upstream `main` is `10188f28`, 14 commits ahead of the fork's `main` (`6ece6f79`), which has not diverged.
- 13 files that upstream changed since then are changed by our branches too: `CHANGELOG.md`,
  `KafkaBundle.properties`, `ccloud/fetcher/DataPlaneFetcher.kt`, `DataPlaneFetcherImpl.kt` (+ test),
  `client/KafkaClient.kt`, `consumer/client/CCloudConsumerClient.kt` (+ test), `data/CCloudClusterDataManager.kt`,
  `data/KafkaDataManager.kt`, `producer/editor/KafkaProducerEditor.kt`, `registry/KafkaRegistryUtil.kt` and
  `registry/confluent/ConfluentRegistryClient.kt`. The upstream changes behind them: #652 bounds the Schema Registry
  calls and makes the HTTP timeouts explicit, #651 bounds the TCP probe before connecting, #657, #659, #660, #661 and
  #669 retry Confluent Cloud requests, #656 reports producer send timeouts.
- `CONTRIBUTING.md`: no pull request without an associated issue; introduce yourself and how you use the plugin;
  self-review (there is a `/pr-review` skill in `.claude/skills`); click-test happy and sad paths; a title that reads
  well in a changelog; a summary without many bullet points; screenshots for UI changes; for a contribution in several
  branches, say how each piece is used later. `AI_POLICY.md`: AI is welcome, slop gets closed (massive pull requests,
  long bullet lists, new patterns without reason, pull requests without an issue).
- Pull requests of outside contributors do get merged, features too (e.g. #646 MessagePack, #636).
- No upstream issue covers OAuth for the Schema Registry yet. #65 (OAUTHBEARER with a custom SASL login callback) is
  related, but about Kafka itself.
- The plugin is Apache-2.0: building and using a fork is allowed.

### Steps

1. Fast-forward the fork's `main` to upstream `main` and push it.
2. Rebase `oauth-4-schem-reg` onto the new `main`, then every other branch onto its rebased parent:
   `git rebase --onto <new parent> <old parent commit from the branch table> <branch>`, or the whole stack at once with
   `--update-refs`. Rebase `backup/all-optimizations` (one commit on top of `oauth-4-schem-reg`) as well and resolve
   its conflicts once: it is the combined code of everything, so afterwards it is again the exact reference for every
   file. Resolve the conflicts of the branches in the files above, run the full suite for every branch, and push with
   `--force-with-lease` (ask first).
3. Build `integration` from the rebased branches in the order of the next section and test it;
   `git diff integration backup/all-optimizations` must show only the two intended differences. Push it. From then on
   the user builds and installs that one.
4. Draft one upstream issue for the user to post: who the user is and how they use the plugin, the problems (no OAuth
   for the Schema Registry; loading, detecting and listing schemas is slow with thousands of subjects), and the
   planned series of small pull requests along the branch table. Ask the maintainers which pieces they want and in
   which order.
5. Then one pull request at a time, each linked to the issue, rebased on upstream `main`, following
   `.github/pull_request_template.md`. First the small fixes (L2, L5, L6, L3, L12) and OAuth (O; ask whether to reword
   its first commit "adding oauth funct."), then the rest, the stack layer by layer, each after its parent is merged,
   saying in the description what comes next. Expect requests to split the big ones: S1 (+3.1k lines), L14 (+2.2k)
   and S5 (+2.0k), most of it tests.
6. A merged pull request drops out of `integration` at its next rebuild. Declined ones, and ones without an answer
   (ping after 2–3 weeks), stay in the fork only.

Cautions:

- Before step 4 the user checks that their employer or client agrees to contributing this code.
- Own builds: `./gradlew buildPlugin`, then Settings → Plugins → Install Plugin from Disk. Turn off automatic plugin
  updates, or skip the update of this plugin, otherwise the next Confluent release replaces the build. Never publish it
  under Confluent's plugin ID or name.

## Merge order and conflicts

This order and the conflict table are what building `integration` needs, and any other merge of all branches. The
fork's `main` is never merged into; it stays a mirror of upstream. After the rebase onto upstream (step 2 above),
expect the conflicts to have moved a little, and use only the rebased backup as reference: the old one lacks the
upstream changes, so taking a file from it would drop them.

### Order

1. `oauth-4-schem-reg`. It must stay first and keep `6e65e118`: its Kafka 4.1.2 upgrade breaks Start Consuming and the
   consumer's Advanced Settings without that fix (see Technical knowledge).
2. Branches without code conflicts: L1, L2, L9, L3, L5, L6, L12, L7, L8.
3. The stack S0 → S1 → … → S6, then L10.
4. Branches that collide with the stack: L4, L13, L11.
5. L14 last.

Why: in this order the stack, the biggest part, goes in unchanged, and every code conflict comes up in a small
single-commit branch. L14 goes last because it collides with the most branches; at that point every other branch that
touches its files is merged, so the backup version of each such file is the exact resolution.

### Conflicts to expect in this order

`CHANGELOG.md` conflicts in almost every merge, because all branches add lines to the same Unreleased sections: keep
the lines of both sides. To have git do that, put `CHANGELOG.md merge=union` into `.git/info/attributes` before merging
or rebasing, then look for duplicated `### …` headers. Delete that line afterwards.

The code conflicts are all of the kind "both sides changed neighbouring lines"; the backup shows the combined code of
every file. When the branch being merged is the last unmerged branch that touches a file (see the appendix), the
resolution is exactly `git checkout backup/all-optimizations -- <file>`.

| Merging | File                                                          | Resolution                                                                                                                                                                                                                                                                                            |
|---------|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| L4      | `client/KafkaClient.kt`                                       | Backup version. Combined form: `val virtualTopics = thisLogger().timed("Checking ${topicNames.size} topics for virtual ones") { virtualTopicDetector.virtualAmong(topicNames) }`                                                                                                                        |
| L13     | `registry/confluent/ConfluentRegistryClient.kt`               | Backup version without L14's `listSchemaUsages(...)` and its `SchemaUsage` import                                                                                                                                                                                                                     |
| L11     | `data/BaseClusterDataManager.kt`                              | Backup version without L14's parts: `SchemaUsageSource` in the class header, the `schemaUsages` property, `getSchemaUsages`, `getCachedSchemaUsages`, `loadSchemaUsages`, `schemaUsages.clear()` in `invalidateSchemaNames()`, and the `SchemaUsage` import                                         |
| L11     | `producer/editor/KafkaProducerFieldComponent.kt`              | Backup version: `KafkaEditorUtils.selectSchemaByName(...)` from S1 followed by `schemaVersionBox.restore(subject, …)` from L11                                                                                                                                                                         |
| L11     | `docs/kafka-plugin-manual.md`                                 | Keep both paragraphs. That is the backup version without L14's paragraph "If the record was written with a Schema Registry schema, …"                                                                                                                                                                 |
| L14     | `BaseClusterDataManager.kt`, `ConfluentRegistryClient.kt`, `KafkaRecord.kt` (+ test), `ConsumerFilter.kt` (+ test), manual | Backup version of each                                                                                                                                                                                                                                         |

Code paths are below `src/io/confluent/intellijplugin/`. This table comes from pairwise `git merge-tree` checks and from the
merge of all branches. The intermediate states of this order have not been built yet: after each merge that needed a
resolution, run the full test suite (see Verification). A cheap first step is to replay the order locally on a
throwaway branch and test after every resolved conflict.

### If the fork becomes the main line

Only if the user gives up on upstream: merge the branches into the fork's `main` in this order with
`git merge --no-ff <branch>`, resolving the conflicts in the merge commits. Pull requests in the fork with base `main`
are then marked as merged when `main` is pushed, without rebasing or force-pushing any branch. For merges on GitHub
instead, open the stacked pull requests (S1–S6, L9, L10, L14) with their parent branch as base and merge with a merge
commit or "Rebase and merge", not with squash; deleting a merged branch retargets the pull requests based on it.
A pull request into upstream can not use a fork branch as its base, which is why the upstream plan goes one pull
request after the other (`git remote add upstream https://github.com/confluentinc/intellij.git`).

## Verification

Temurin JDK 21 was used for all runs; `CLAUDE.md` asks for JDK 21+, and `.sdkmanrc` pins 24.0.2-tem and Gradle
8.14.3. The commands unset `JAVA_TOOL_OPTIONS`, which an IDE terminal may set; that is harmless when it is not set.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home   # macOS, Temurin 21
env -u JAVA_TOOL_OPTIONS ./gradlew cleanTest test --no-build-cache --continue \
  -Pkotlin.daemon.jvmargs=-Xmx8g --console=plain > /tmp/test.log 2>&1
grep "^e: " /tmp/test.log                                  # compile errors, if any
python3 - <<'EOF'
import glob, xml.etree.ElementTree as ET
tests = failed = skipped = 0
for path in glob.glob("build/test-results/test/TEST-*.xml"):
    suite = ET.parse(path).getroot()
    tests += int(suite.get("tests")); skipped += int(suite.get("skipped"))
    for case in suite.iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            failed += 1; print("FAIL", case.get("classname"), case.get("name"))
print("tests", tests, "failed", failed, "skipped", skipped)
EOF
env -u JAVA_TOOL_OPTIONS ./gradlew buildPlugin              # build/distributions/*.zip
```

Two tests are always skipped: `ConnectionDataTest.connectionDataFields` and
`SerializablePropertiesTest.testReadsDirectlyBackingField`.

**How the split was proven.** All branches were merged one after the other into a throwaway branch, each conflict
resolved with `git checkout backup/all-optimizations -- <file>`; the result differed from the backup only by the
intended differences below. A second check compared the lines that each branch commit adds and removes with the lines
the backup adds and removes: nothing was lost, nothing was added. The stack was made by removing one layer after the
other from the top state, with the full suite after each layer.

**Intended differences from the backup.**

1. `test/.../producer/client/VersionedSchemaSerializationTest.kt`: on L11 the v2 value is written in Avro JSON
   (`{"note":{"string":"hi"}}`), because the plain form the backup uses (`"note":"hi"`, as AKHQ shows it) can only be
   read with L9. Both pass once L9 and L11 are merged.
2. `CHANGELOG.md`: the backup says the schema names are kept "for a minute instead of 15 seconds"; the branches drop
   "instead of 15 seconds", because no released version had a 15-second cache.

## Backlog

Open items from the last rated lists. Value, effort and risk: H high, M medium, L low; effort S ≤ 2 h, M about a day,
L more.

| Item                                                                                                                                        | Value | Effort | Risk | Notes                                                                                                                                                                                                                              |
|---------------------------------------------------------------------------------------------------------------------------------------------|-------|--------|------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Use the IDE trust store for Schema Registry TLS (corporate CAs)                                                                             | M–H   | M      | M    | Verdict was "do it if PKIX errors come back". It only helps the REST calls: the OAuth token endpoint goes through Kafka's own JWT retriever. Until then L12 explains the error and the manual says that a custom truststore replaces the default one. |
| Plugin Verifier in CI (253 and latest; IU, WebStorm, PyCharm, GoLand, Rider)                                                                | M     | S–M    | L    | Its own pull request. The 253 release shipped a `NoClassDefFoundError`; MigLayout is scheduled for removal (IDEA-306719) and used in 12 files.                                                                                   |
| De-duplicate the data-manager flows (`deleteSchema` is copy-pasted) and extract a testable schema catalog                                   | M     | L      | M–H  | Skipped so far. The data managers can not be built in tests, so the cache invalidation is only covered indirectly.                                                                                                                 |
| Merge the Kafka and Confluent Cloud schema-detail controllers (about 230 of 405 lines shared) and move them from MigLayout to the UI DSL     | L–M   | L      | M    | Skipped so far.                                                                                                                                                                                                                    |
| Run the registry check in parallel with `describeCluster`; the TCP probe runs twice                                                         | L     | S–M    | M    | Skipped: little is left to gain since L13. Upstream #651 changed the probe; check what is left after the rebase.                                                                                                                  |
| Delete about 28 unreferenced files (about 830 lines, e.g. `DateFilterPopupComponent`, `ClustersStorage`)                                    | L     | S      | L–M  | Skipped: would cause merge conflicts with upstream.                                                                                                                                                                                |
| The random-data wand still writes Avro JSON with union wrappers                                                                             | L     | M      | M    | Since L9 the producer also accepts the plain form.                                                                                                                                                                                 |
| Remaining deprecated API uses (e.g. `Disposer.isDisposed`)                                                                                  | L     | S      | L    | The ones this work introduced are fixed; older ones are left.                                                                                                                                                                      |
| The user turns on DEBUG logging for `#io.confluent.intellijplugin` (Help → Diagnostic Tools → Debug Log Settings)                           | M     | –      | none | Shows why a schema was selected and the timings under one second.                                                                                                                                                                  |

Known limitations:

- Covered by unit tests but not yet tried in a running IDE: the schema rows in the record details (L14), the order of
  the editor list (S4), the `^prefix` filter (S5), and the topic lookup, 5-minute cache and diagnostics (S6).
- Whether the user's registry supports `subjectPrefix` is not confirmed. If it answers with a refusal, or ignores the
  parameter (subjects outside the prefix in the answer), the client treats prefix listing as unsupported and lists
  everything as before.
- The schema tab of a topic trusts the last full listing whatever its age; the 5-minute cache widens that window.
- Confluent Cloud: updates of a single schema bypass the filtered view, so the count of hidden schemas can be off by
  one.
- A record that names its schema by GUID (wire format V1) shows the GUID only.
- A `bytes` field whose JSON is also valid Avro JSON is read as the characters; documented in the manual, kept on
  purpose.

## Technical knowledge

- **Kafka clients 4.1.2 and the class loader.** The Schema Registry client's OAuth token provider needs Kafka 4.1.
  Kafka's `ConfigDef` resolves the default of `sasl.oauthbearer.jwt.retriever.class` (`DefaultJwtRetriever`) with the
  thread's context class loader. On the UI thread that loader does not see the plugin's classes, so `ConsumerConfig`
  fails to initialise for the rest of the IDE session (`NoClassDefFoundError: Could not initialize class …ConsumerConfig`
  on Start Consuming and Advanced Settings). `util/KafkaConfigDefs.kt` builds the config definitions with the plugin
  class loader (`withPluginClassLoader`). New code that touches Kafka config classes on the UI thread must do the same.
- **OAuth token.** `registry/confluent/SingleFlightBearerAuthCredentialProvider.kt` (`SingleFlightBearerAuthCredentialProvider`,
  `SingleFlightTokenRestService`) lets concurrent registry calls share one token request.
- **Big registries.**
  - One listing of the schema names is shared by the editors and the tool window (`data/ExpiringSingleFlight.kt`). It is
    kept a minute, or 5 minutes when the registry has shown that it can list by prefix. The editors show the names they
    have at once and correct them when the registry answers.
  - Formats are looked up on demand, at most 5 at a time, the selected schema and the topic's schema first
    (`data/SchemaFormatResolver.kt`). A failed lookup is not repeated for a minute.
  - `CachedSchemaRegistryClient` keeps 1000 schemas instead of 100.
  - Prefix listing: `ConfluentRegistryClient.listSchemasByPrefix`, cached per prefix in `data/PrefixListings.kt`;
    `data/SchemaListingCache.kt` decides which listing serves what.
  - The tool window section is built by `data/SchemaRegistryList.kt` from the cached listing.
  - The connection check asks for one subject (`getAllSubjectsWithPagination(0, 1)`) and falls back to the full list
    when the registry refuses paging.
- **Cancellation** (`ProcessCanceledException`, `CancellationException`) must be rethrown and never logged as a failure:
  `util/CatchFailures.kt`. `ObjectDataModel` keeps the data of the first phase when the second one is cancelled.
- **Logging.** `util/LogTiming.kt`: `timed` logs at INFO from 1 second on, below that at DEBUG; `timedOnEdt` tells the
  wait for the UI thread apart from the work.
- **Schema ID of consumed records.** The Confluent deserializers are wrapped (`SchemaIdRecordingDeserializer`) to note
  the ID in reserved headers (`SchemaIdHeaders` in `registry/RecordSchema.kt`), which the record table and the header
  filter leave out. `data/RecordSchemaLookup.kt` asks the registry once per ID.
- **Test helpers:** `util/RecordingLogger` (test sources) to assert log lines; a child-first class loader in the
  `KafkaConfigDefs` tests.

## Continue on another machine

Everything the work needs is on GitHub. The chats are not: they are in `~/.copilot/` (GitHub Copilot CLI; this
session is `72e60e92-991e-49a9-804c-4ef9a5d8f3af`), `~/.junie/` (Junie), `~/.config/github-copilot/` (Copilot in the
IDE) and `~/Library/Application Support/JetBrains/` (IDE settings, AI Assistant). macOS Migration Assistant moves all
of them. Otherwise copy them with the IDE and the CLI closed, never through git or a cloud drive, because they hold
login tokens. With the same user name and the repository at the same path as before, `copilot --resume 72e60e92`
should continue that session with its memory (not tried across machines; this note works either way).

```bash
git clone https://github.com/mwiltzer/intellij.git && cd intellij
git switch handoff/pr-split            # this note plus the complete code
# With jenv: point .java-version at a Temurin 21 that `jenv versions` lists. On the old machine it held
# "temurin64-21.0.12.1". Never commit that file.
```

A prompt for the new agent:

> This is my fork of the Confluent Kafka plugin. Check out the branch `handoff/pr-split` and read `HANDOFF.md`
> completely. Then continue with the upstream plan described there, starting with step 1. Ask me before pushing,
> force-pushing, opening pull requests or merging.

## Appendix: which branch owns what

Files that several branch commits touch, without `CHANGELOG.md` (code below `src/io/confluent/intellijplugin/`, tests
below `test/io/confluent/intellijplugin/`, other paths from the repository root). Use it to decide whether the backup
version of a file is the exact resolution.

| File                                                    | Branches                                     |
|---------------------------------------------------------|----------------------------------------------|
| `client/KafkaClient.kt`                                 | O, L4, L12, S0, S3                           |
| `common/editor/KafkaEditorUtils.kt`                     | S1, S6, L10 (its test: S1, L10)              |
| `common/editor/SchemaComboBoxLoader.kt` (+ test)        | S1, S4, S6                                   |
| `consumer/client/CCloudConsumerClient.kt`               | L8, L14                                      |
| `consumer/editor/KafkaConsumerPanel.kt`                 | L8, S1, L14                                  |
| `consumer/editor/KafkaRecord.kt` (+ test)               | L7, L14                                      |
| `consumer/models/ConsumerFilter.kt` (+ test)            | L8, L14                                      |
| `consumer/models/ConsumerProducerFieldConfig.kt`        | L8, L9, L11                                  |
| `data/BaseClusterDataManager.kt`                        | L11, S0–S6, L14                              |
| `data/CCloudClusterDataManager.kt`                      | S0, S1, S2, S3, L14                          |
| `data/ClusterDataFilters.kt`                            | S2, S3, S5 (its test: S2, S5)                |
| `data/ExpiringSingleFlight.kt` (+ test)                 | S1, S2, S6                                   |
| `data/KafkaDataManager.kt`                              | S0, S1, S2, S3, S5, L14                      |
| `data/PrefixListings.kt`, `SchemaListingCache.kt` (+ tests) | S5, S6                                   |
| `data/SchemaFormatResolver.kt` (+ test)                 | S1, S2                                       |
| `data/SchemaFormats.kt`                                 | S1, S4, S6                                   |
| `data/SchemaListSettingsReader.kt` (+ test)             | S2, S3                                       |
| `data/SchemaRegistryList.kt` (+ test)                   | S2, S3, S5                                   |
| `docs/kafka-plugin-manual.md`                           | O, L8, L9, L11, L12, S1–S6, L14              |
| `gradle/libs.versions.toml`                             | O, L1                                        |
| `producer/client/CCloudProducerClient.kt`               | L7, L9, L11 (its test: L9, L11)              |
| `producer/editor/KafkaProducerFieldComponent.kt`        | L11, S1, L10                                 |
| `registry/confluent/ConfluentRegistryClient.kt`         | O, L13, S1, S2, S5, L14                      |
| `registry/confluent/controller/KafkaRegistryController.kt` | S2, S3, S5                                |
| `resources/messages/KafkaBundle.properties`             | O, L8, L11, L12, S1, S3, S5, L14             |
| `util/LogTiming.kt`                                     | S0, S6                                       |

What the stack layers add to the shared files, for work inside the stack:

- `ExpiringSingleFlight`: S1 `get`/`invalidate`/`peek` with a fixed TTL; S2 `refresh`; S6 `peekFresh` and a TTL that
  is read at every access.
- `SchemaFormatResolver`: S1 formats; S2 the details cache (`knownInfo`, `detailsOf`, `infoTtlMillis`).
- `SchemaFormats`: S1 names, cached names, known and resolved formats; S4 `getPinnedSchemaNames`; S6
  `getSchemaNamesForTopic`, `getSchemaNamesToFindTopicSchemas`.
- `SchemaRegistryList.kt`: S2 `SchemaListing` (names, contains) and the list with view, refilter and merging of
  details; S3 the hidden counts and the preparation that loads the visible topics; S5 prefixes (`SchemaListing`
  coverage, `SchemaListings`, fetching by prefix with a settle delay).
- `ClusterDataFilters`: S2 `matchesName` (contains, ignoring case) and `schemaListView`; S3 hiding
  (`isInternalSubject`, `withoutContext`, `topicOfSubject`, hidden counts); S5 `prefixOf` and the `^` branch of
  `matchesName`.
- `SchemaComboBoxLoader`: S1 the loader; S4 the order (`SchemaOrder`) and pinned names; S6 the early topic lookup,
  `timedOnEdt` and the debug reason of the selection.
- `BaseClusterDataManager`: S0 `catchFailures` for topic and schema details; S1 the schema scope, format resolver,
  names flight and `SchemaFormats`; S2 the listing, `SchemaRegistryList`, `refilterSchemas`, `schemaExists` from the
  listing; S3 visible topics and hidden counts; S4 pinned names; S5 prefix listings, `checkSchemaExists`, tri-state
  `schemaExists`; S6 the names flight with the dynamic TTL and `getSchemaNamesForTopic`; L14 schema usages; L11
  `getSchemaVersionsForEditor`.
- `ConfluentRegistryClient`: S1 cache capacity and `logLoadFailure`; S2 `listSchemas()` without filter and limit; S5
  `listSchemasByPrefix`; L13 `checkConnection`; L14 `listSchemaUsages`. L13, S5 and L14 each turn the `restService`
  parameter into a property, the same change, which merges cleanly.
- `KafkaRegistryController`: S2 `refilterSchemas` instead of reloading; S3 switches, hidden note, empty texts; S5
  placeholder and tooltip of the filter field.
