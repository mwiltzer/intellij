# Handoff: OAuth for Schema Registry and performance work, split into pull requests

State of 2026-11-13, after the rebase onto upstream `main` (`ca42fde6`). This file exists only on the branch
`handoff/pr-split`. That branch is the complete result (identical to `backup/all-optimizations`) plus this note and
the pointers to it in `AGENTS.md`, `CLAUDE.md` and `.github/copilot-instructions.md`. **Never open a pull request from
it, never merge it, and never base new work on it.**

## In one minute

- The fork `mwiltzer/intellij` (public, forked from `confluentinc/intellij`) adds OAuth 2.0 client credentials for
  Confluent Schema Registry (branch `oauth-4-schem-reg`) and about twenty performance and usability improvements. Most
  of them are for Schema Registries with more than 10,000 subjects, where listing all subjects takes 4–5 seconds.
- The improvements were developed as one bundle, which is kept as `backup/all-optimizations`. That bundle was split
  into **21 branches with one commit each** on top of `oauth-4-schem-reg`, so that each improvement can be its own pull
  request. All branches are pushed to `origin`. No pull request exists yet.
- One more branch, `fork/idea-2026.3-charts` (F1), is for the fork only and never goes upstream: it makes the built
  plugin load in IntelliJ IDEA 2026.3, which upstream #675 broke (see "Facts"). The backup contains it, so a build from
  this branch requires IDEA 2026.3.
- Every branch passes the full test suite on its own. Merging all of them gives exactly the backup, apart from the two
  intended differences listed below; the backup passes all 3,026 tests and `buildPlugin`.
- **Decided on 2026-11-11: upstream first, the fork as fallback.** The pull requests go to `confluentinc/intellij`;
  whatever upstream does not take lives on in the fork. Steps 1 and 2 of the upstream plan below are done: the fork's
  `main` equals upstream `main`, and every branch is rebased onto it and tested. **Next step:** step 3.
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
├── oauth-4-schem-reg                                  O
│   ├── L1 L3 L4 L5 L6 L7 L8 L11 L12 L13               independent, one commit each
│   ├── L2 ── L9
│   └── S0 ── S1 ── S2 ── S3 ── S4 ── S5 ── S6         stack, one commit each
│             ├── L10
│             └── L14
└── F1                                                 fork only, never a pull request
```

| ID  | Branch                                 | Based on | Commit     | What it does                                                                                                                                                                                                                                                  | Tests        |
|-----|----------------------------------------|----------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| O   | `oauth-4-schem-reg`                    | `main`   | `9a7a8ec2` | 4 commits: the user's OAuth commit `17e96f2f` (includes Kafka clients 4.0.0 → 4.1.2), `c0de4f9a` Kafka config classes built with the plugin class loader, `7c001103` one token request for concurrent calls, `9a7a8ec2` OAuth fields checked by Test Connection | 2,124        |
| L1  | `chore/dependency-bumps`               | O        | `3669de97` | moshi 1.15.2, JUnit 5.14.4 / platform 1.14.4, mockito-kotlin 6.4.0, kotlin-metadata 2.2.21, Sentry Gradle plugin 5.12.2, compiler flag `-Xannotation-default-target=first-only`                                                                                | 2,124        |
| L2  | `fix/avro-fixed-decimal`               | O        | `3a31210f` | Random data for an Avro decimal stored as `fixed` failed                                                                                                                                                                                                      | 2,126        |
| L3  | `fix/ccloud-cache-concurrency`         | O        | `7e7caf99` | Confluent Cloud caches safe for concurrent use                                                                                                                                                                                                                | 2,137        |
| L4  | `perf/virtual-topic-detection`         | O        | `69f10b0f` | Virtual topics are detected once per topic; a topic the user may not describe no longer breaks the detection                                                                                                                                                  | 2,148        |
| L5  | `fix/install-notification-per-project` | O        | `2b9d34c0` | One "plugin installed" notification per open project (the shared one caused a SEVERE "BalloonImpl already disposed")                                                                                                                                         | 2,129        |
| L6  | `fix/bdt-url-handler-classcast`        | O        | `31dfc283` | No `ClassCastException` stack traces in `idea.log` when Big Data Tools is installed                                                                                                                                                                           | 2,131        |
| L7  | `perf/record-table-memory`             | O        | `8057f537` | Record tables keep only the text of keys and values (about 5× less heap)                                                                                                                                                                                      | 2,126        |
| L8  | `perf/consumer-filter`                 | O        | `4269ac14` | Consumer filter: regex compiled once, checked while typing, matched against the text shown                                                                                                                                                                    | 2,157        |
| L9  | `feat/avro-json-replay`                | L2       | `b015c012` | Produce Avro values from JSON as tools like AKHQ show it, to replay records                                                                                                                                                                                   | 2,216        |
| L11 | `feat/producer-schema-version`         | O        | `b3410fcc` | The producer can write with an older version of the registry schema                                                                                                                                                                                           | 2,175        |
| L12 | `fix/registry-error-hints`             | O        | `f5983c34` | Says why the Schema Registry is not available (untrusted TLS certificate, rejected OAuth token, …); manual: a custom truststore replaces the default one                                                                                                       | 2,143        |
| L13 | `perf/registry-connection-check`       | O        | `131c10c6` | The connection check asks the registry for one subject instead of all                                                                                                                                                                                         | 2,133        |
| S0  | `perf/log-slow-loads`                  | O        | `e9ed15f4` | Slow topic loads (≥ 1 s) logged at INFO; a cancelled load ends quietly instead of being logged and shown as an error                                                                                                                                          | 2,141        |
| S1  | `perf/editor-schema-loading`           | S0       | `eac6ccd0` | Editors: one shared listing of the schema names (kept a minute, older names shown while refreshing), formats looked up on demand and a few at a time, saved schema kept by name, CCloud names-only entries no longer taken for Avro, SR client cache 1000 | 2,273        |
| S2  | `perf/schema-registry-section`         | S1       | `7924c693` | The Schema Registry section is shown from one listing; filter, limit and favorites re-slice it; the filter is a case-insensitive "contains" instead of a regex                                                                                                | 2,346        |
| S3  | `feat/hide-internal-and-other-schemas` | S2       | `ba1c5b34` | Hides the schemas of internal topics and of topics the user can not see, with two switches (default off)                                                                                                                                                     | 2,426        |
| S4  | `feat/editor-schema-order`             | S3       | `57c32378` | The editors list the schemas that matter first (the topic's schemas, favorites, then the rest, internal ones last)                                                                                                                                            | 2,454        |
| S5  | `feat/schema-prefix-filter`            | S4       | `90a08330` | `^prefix` filter in the Schema Registry section, served by `GET /subjects?subjectPrefix=`                                                                                                                                                                      | 2,578        |
| S6  | `perf/editor-topic-schema-lookup`      | S5       | `e62b8d67` | Editors find the topic's schema by prefix without waiting for the full listing; names kept 5 minutes where prefix listing works; logs of the UI-thread wait and of the selection reason                                                                       | 2,649        |
| L10 | `fix/schema-combo-width`               | S1       | `3fbd5658` | Long schema names no longer push the buttons next to the schema list out of view                                                                                                                                                                              | 2,275        |
| L14 | `feat/record-schema-id`                | S1       | `2c0c0a45` | The details of a consumed record show the schema ID and the subjects and versions it is registered as (called C3 in the session notes)                                                                                                                        | 2,393        |
| F1  | `fork/idea-2026.3-charts`              | `main`   | `97f9ac6d` | Fork only, never a pull request: the built plugin requires `intellij.charts.shared` instead of `intellij.charts`, so that it loads in IntelliJ IDEA 2026.3 (broken by upstream #675). Compiling and the tests stay on 2026.1                                  | 2,028        |
| –   | `backup/all-optimizations`             | O        | `e86e4704` | The whole bundle as one WIP commit: the target state and the reference for resolving conflicts. Never merge it.                                                                                                                                              | 3,026        |
| –   | `handoff/pr-split`                     | backup   | –          | The backup plus this note. Never merge it.                                                                                                                                                                                                                   | –            |

The backup, which is all of them together: **3,026 tests, 0 failures, 2 known skips, `buildPlugin` OK.** Every branch
has its own tests plus all of upstream's. The commit message of each branch is written to serve as the description of
its pull request. The commit IDs are those after the rebase onto upstream `main` `ca42fde6` (see "Keeping up with
upstream"); earlier versions of this file list the ones before. Once `integration` exists, it replaces
`handoff/pr-split` as the branch with everything.

## Upstream plan (decided 2026-11-11)

The user wants the work in the original repository, `confluentinc/intellij`. Whatever the maintainers do not want stays
in the fork. The fork is set up so that this needs no switch later:

- The fork's `main` stays a plain mirror of upstream `main`: only fast-forwarded, never committed to.
- A branch `integration` (still to be created) is upstream `main` plus all branches, merged in the order of the next
  section. It is the plugin the user builds and installs, from the first day on. Rebuild it whenever upstream `main`
  or a branch changes; a branch that upstream merged no longer adds anything to it.

### Facts on 2026-11-11

- Upstream `main` was `10188f28`, 14 commits ahead of the fork's `main` (`6ece6f79`), which had not diverged. Since
  step 1, the fork's `main` follows upstream `main`.
- Later the same day upstream `main` moved to `756fb86c`: #675 declares the `intellij.charts` module for producing
  from CSV, #658 stops the Confluent Cloud token refresh on terminal errors. They change Confluent Cloud sign-in code,
  `TelemetryEvent.kt` and `plugin.xml`, none of our files apart from `CHANGELOG.md`, and no code that our branches
  use. The fork's `main` and all branches were moved onto it as described in "Keeping up with upstream" below.
- **#675 breaks IntelliJ IDEA 2026.3** (released 2026-11-05, build 263.7307.200, which the user runs). It makes the
  module `intellij.charts` a required dependency in `plugin.xml`. In 2026.1, the build target, that module is a product
  module in `lib/intellij.charts.jar`, on the core class path; that is why installed builds never needed the
  declaration and only `runIde` failed (#469). 2026.3 has no such module: the same classes are in
  `intellij.charts.shared`, a content module (`visibility="public"`) of the bundled Charts plugin
  `com.intellij.platform.charts`. So 2026.3 does not load the plugin at all: "Plugin 'Confluent' cannot load because
  required module 'intellij.charts' is not available". The plugin declares no until-build (the build sets since-build
  to 261.22158), so upstream's next release will fail the same way for every 2026.3 user. Not reported upstream yet;
  it is a small, useful first issue for the user to post. F1 works around it in the fork only.
- On 2026-11-12 upstream `main` moved twice more, to `ae3b47c0` and then `c69c01a0`. #667 stops the admin client's
  `client.id` from leaking into the producer and consumer configs (`KafkaClientBuilder`). #662 drops stacked 429
  retries of the Confluent Cloud producer and reports when retries run out; it reworks the run state of
  `CCloudProducerClient` and moves `createErrorRecord` from `KafkaProducerClient.kt` to `ProducerClient.kt`. #677 lets
  Stop interrupt the rate-limit backoff: `CCloudRestClient.executeRequest` got `completeOnceSent`, which
  `DataPlaneFetcherImpl.produceRecord` sets, instead of the `withContext(NonCancellable)` in `CCloudProducerClient`.
  L7, L9 and L11 change other parts of `CCloudProducerClient` (record text, Avro JSON, schema versions), L14 other
  parts of `DataPlaneFetcherImpl`, and none of our branches use what upstream changed. Both rebases had no conflict
  apart from `CHANGELOG.md`, and every branch compiled and passed its tests.
- On 2026-11-13 upstream `main` moved to `ca42fde6`: #672 refreshes Confluent Cloud tokens at 70–90 % of their real
  lifetime, #643 skips unserializable connection properties instead of failing the whole save, #678 derives the AWS
  regions from the SDK, and #668 refactors the tests. #668 is the one that mattered: it moved the test fixtures to
  `test/resources/fixtures/<area>/` (`ccloud-resources-mock-responses/` became `fixtures/ccloud-resources/`,
  `scaffold-mock-responses/` became `fixtures/scaffold/`), deleted `ccloud/util/ResourceLoader.kt`, and replaced the
  per-class WireMock servers with shared helpers (see "Test helpers" under Technical knowledge). Two branches needed
  changes: L11 had an import conflict in `KafkaRegistryUtilTest` (upstream's imports plus L11's three). L14's tests in
  `DataPlaneFetcherImplTest` called `loadMockResponse` and `wireMockServer`, which no longer exist; git reported only
  an import conflict there, the compiler the rest. They now call `ccloudResourceFixture` and `wireMock`, and L14's
  fixture `schema-versions-by-id.json` moved to `fixtures/ccloud-resources/` (unchanged). The backup got the same
  files as L11 and L14, the only branches that touch them.
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

1. **Done on 2026-11-11.** Fast-forward the fork's `main` to upstream `main` and push it.
2. **Done on 2026-11-11.** Rebase `oauth-4-schem-reg` onto the new `main`, then every other branch onto its rebased
   parent: `git rebase --onto <new parent> <old parent commit from the branch table> <branch>`, or the whole stack at
   once with `--update-refs`. Rebase `backup/all-optimizations` (one commit on top of `oauth-4-schem-reg`) as well and
   resolve its conflicts once: it is the combined code of everything, so afterwards it is again the exact reference for
   every file. Resolve the conflicts of the branches in the files above, run the full suite for every branch, and push
   with `--force-with-lease` (ask first).
   What it took: `CHANGELOG.md` conflicted in every branch (both sides kept, with the union rule described below).
   The code conflicts were imports and neighbouring additions only: `KafkaRegistryUtil` (L11), `ConfluentRegistryClient`
   (L13, S1; upstream's `DEFAULT_HTTP_PROPS` and `mergeProps` first, then our constants), `KafkaDataManager` (S1, L14),
   `CCloudClusterDataManager` and `DataPlaneFetcherImplTest` (L14), and the same files in the backup. Every commit
   adds and removes the same lines as before, except two imports: S1 keeps `kotlinx.coroutines.CancellationException`
   in `KafkaDataManager`, because upstream's consumer group deletion uses it, and L14 no longer adds `CCloudApiException`
   to `DataPlaneFetcherImplTest`, because upstream imports it already. Watch for that kind of conflict in the next
   rebases: git reports no conflict when one side deletes an import that new code of the other side needs; only the
   compiler does. The OAuth client is built from upstream's merged properties (`mergeProps`), so the explicit HTTP
   timeouts of #652 apply to it too. The heads before the rebase were kept as local tags `pre-sync/<branch>` until
   everything was pushed and checked, and then deleted. On the machine that did the rebase, `git reflog <branch>`
   lists them until about 2026-12-11.
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
  under Confluent's plugin ID or name. A build that contains F1 (the backup, `handoff/pr-split`, `integration`) loads
  only in IDEs with the module `intellij.charts.shared`, such as 2026.3; `runIde`, which starts 2026.1, does not load
  it. Builds of the other branches load in 2026.1 and not in 2026.3.

### Keeping up with upstream

Upstream `main` moves often: two commits arrived within hours of the first rebase, three more on the next day, four
the day after. Each move took about an hour, most of it tests. The last one, onto `ca42fde6`, went like this:

1. `git fetch upstream` and `git fetch origin`. Look at what upstream changed since the old base, and at the files it
   shares with the backup: `git diff --name-only <old base> upstream/main` against
   `git diff --name-only <old base> backup/all-optimizations`. Check whether our code uses anything upstream changed:
   collect the names that upstream's removed lines contain and its new files no longer do, and look for them in the
   lines our commits add. That found the two deleted test helpers L14 used (see "Facts").
2. Check that every local branch equals its `origin/` copy. Then the `origin/` refs keep the heads from before the
   rebase; the rebase commands, the checks and the lease use them, so no backup tags are needed.
3. In a separate worktree, with `CHANGELOG.md merge=union` in `.git/info/attributes`: rebase O with
   `git -c merge.directoryRenames=true rebase --onto upstream/main origin/main oauth-4-schem-reg`, F1 the same way,
   every other branch with `git rebase --onto <parent> origin/<parent> <branch>`, the stack with `--update-refs` up to
   S6, then the backup and `handoff/pr-split`. `merge.directoryRenames=true` moves a file that one of our commits adds
   to a folder that upstream renamed along into the new folder, instead of stopping with a "file location" conflict.
   Resolve code conflicts as in step 2 above, and compile a branch whose tests or code needed changes
   (`./gradlew compileTestKotlin`) before resolving the backup the same way. Delete the union line afterwards.
4. Check: the Unreleased section of every `CHANGELOG.md` has each `### …` header once and no line twice; every commit
   adds and removes the same lines as its `origin/` copy, apart from the adaptations made on purpose; the merge of all
   branches equals the backup apart from the intended differences (see Verification).
5. Run the full suite for upstream `main` and for every branch, one after the other, and `buildPlugin` for the backup.
   `buildPlugin` fails on purpose with "requires neither …" once upstream changes the charts dependency in
   `plugin.xml`: then check whether upstream supports 2026.3 itself and drop F1 (also from the backup), or adapt it.
6. Ask, then push in one `git push --atomic`: `main` as a fast-forward (`upstream/main:main`), the branches with
   `--force-with-lease`, which checks them against the `origin/` refs. Update the commit IDs, test counts and facts in
   this note.

## Merge order and conflicts

This order and the conflict table are what building `integration` needs, and any other merge of all branches. The
fork's `main` is never merged into; it stays a mirror of upstream. The backup was rebased onto upstream together with
the branches (step 2 above), so it is again the reference for every file. Should upstream move on, rebase it with the
branches again and never take a file from an older backup: it would drop the upstream changes.

### Order

1. `oauth-4-schem-reg`. It must stay first and keep its commit "fix: build the Kafka client configs with the plugin
   class loader" (`c0de4f9a`): its Kafka 4.1.2 upgrade breaks Start Consuming and the consumer's Advanced Settings
   without that fix (see Technical knowledge).
2. Branches without code conflicts: L1, L2, L9, L3, L5, L6, L12, L7, L8.
3. The stack S0 → S1 → … → S6, then L10.
4. Branches that collide with the stack: L4, L13, L11.
5. L14 last.
6. F1, anywhere: it changes only a block of `build.gradle.kts` that no other branch touches. It is part of the fork's
   `integration`, never of a pull request.

Why: in this order the stack, the biggest part, goes in unchanged, and every code conflict comes up in a small
single-commit branch. L14 goes last because it collides with the most branches; at that point every other branch that
touches its files is merged, so the backup version of each such file is the exact resolution.

### Conflicts to expect in this order

`CHANGELOG.md` conflicts in almost every merge, because all branches add lines to the same Unreleased sections: keep
the lines of both sides. For a rebase, git can do that: put `CHANGELOG.md merge=union` into `.git/info/attributes`,
rebase, check that every `### …` header of the Unreleased section is there once and no line is doubled, and delete
that line again. That worked for every branch in step 2. In merges the union rule doubled `### Added` headers and
lines, so resolve `CHANGELOG.md` by hand there: the Unreleased section of the backup is the combined result, apart
from intended difference 2.

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
merge of all branches. After the rebase, the order was replayed with `git merge-tree` (without a checkout, each
conflict resolved with the backup version): the same files conflicted as before the rebase, and the manual merged
without a conflict at L11. In that replay, the whole backup version of `ConfluentRegistryClient.kt` at L13 brought in
L14's `listSchemaUsages` early, and L14 then merged without a conflict and added it a second time; so follow the table
at L13, and take the backup version of every file L14 touches at L14. The intermediate states of this order have not
been built yet: after each merge that needed a resolution, run the full test suite (see Verification). A cheap first
step is to replay the order locally on a throwaway branch and test after every resolved conflict.

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

Run test suites one after the other, not two at the same time on one machine: two test classes from upstream then
fail at random, because they rely on ports of the machine. `CCloudRestClientRetryTest` expects "connection refused"
from a port that was free a moment before, and `ScaffoldHttpClientTest` calls its WireMock server through `localhost`.
Even in a serial run, after upstream #668, `CCloudRestClientTransportTest > ParseErrors > fetch parse failure should
propagate parser exception rather than status 0` failed once on L8, which does not touch that code, and passed in two
reruns of the whole suite: rerun a branch before suspecting it when one of upstream's own WireMock tests fails.
Upstream `main` `ca42fde6` alone has 2,028 tests.

**How the split was proven.** All branches were merged one after the other into a throwaway branch, each conflict
resolved with `git checkout backup/all-optimizations -- <file>`; the result differed from the backup only by the
intended differences below. A second check compared the lines that each branch commit adds and removes with the lines
the backup adds and removes: nothing was lost, nothing was added. The stack was made by removing one layer after the
other from the top state, with the full suite after each layer.

**After the rebase onto upstream** (2026-11-11) the same checks were repeated. The lines that each commit adds and
removes are those of before the rebase, apart from the two imports named in step 2 of the upstream plan. The replayed
merge of all branches equals the rebased backup apart from intended difference 1, and the `CHANGELOG.md` lines of the
backup are those of upstream and of the branches, apart from intended difference 2. Every branch and the backup passed
the full suite on its own; the backup also passed `buildPlugin`. After the later rebases, onto `756fb86c`, `ae3b47c0`
and `c69c01a0`, every commit adds and removes exactly the same lines as before it, and the other checks gave the same
results. After the rebase onto `ca42fde6` that holds for every commit apart from L14 (and the backup), whose two test
lines and fixture path were adapted to upstream's test helpers; the moved fixture is byte for byte the same.

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
| Move our new tests to upstream's shared test helpers before their pull requests                                                             | M     | S      | L    | Since upstream #668 the tests use `wireMockExtension()` and `fixture(...)`. Eight of our test files still start their own `WireMockServer`: O (`ConfluentRegistryClientOAuthTest`), L11, L12, L13, L14, S1, S2, S5. Upstream's AI policy closes pull requests with new patterns without reason. |
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

- **Building against IntelliJ IDEA 2026.3** (tried on 2026-11-11, not done): the IntelliJ Platform Gradle Plugin can
  read 2026.3 from version 2.12.0 on (the `namespace` attribute of module descriptors), and 2.12.0 and later require
  Gradle 9. Gradle 9 removed `Project.exec`, which `openApiGenerate` uses (`providers.exec { … }.result.get()` replaces
  it). For 2026.3 the Gradle plugin (2.14 and later) sets the Java toolchain to 25, the JetBrains Runtime of 2026.3, and
  the project's Kotlin Gradle plugin then fails with "Unknown Kotlin JVM target: 25"; a newer Kotlin, or an explicit
  toolchain 21, comes next. So targeting 2026.3 is a migration of its own; F1 avoids it. The chart API that the plugin
  calls (25 members of `DataFrame`, `DataFrameCSVAdapter`, `Column`, `Type` and the column types) has the same
  signatures in 2026.1's `intellij.charts` and 2026.3's `intellij.charts.shared` (checked with `javap`).
- **Tests read the processed `plugin.xml`.** The tests use `build/resources/main/META-INF/plugin.xml`, which
  `patchPluginXml` also writes. With a required module that the test IDE (2026.1) lacks, the plugin is not loaded in
  the tests and 28 of them fail. That is why F1 patches the jar in the sandbox (`prepareSandbox`) instead.
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
  `KafkaConfigDefs` tests. Since upstream #668: `testutil/WireMockTestSupport.kt` (`wireMockExtension()`, registered
  with `@JvmField @RegisterExtension` in a companion object; `wireMock.baseUrl()`, `wireMock.stubFor(...)`),
  `testutil/TestResources.kt` (`fixture(dir, name)` reads `test/resources/fixtures/<dir>/<name>`, `closedPort()`), and
  for Confluent Cloud `ccloud/client/CCloudRestClientTestSupport.kt` (`ccloudFixture`, `ccloudResourceFixture`,
  `testClient`, `signedInAuthService`). New tests should use them, and fixtures belong in `test/resources/fixtures/`.

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
# Remotes are not part of a clone. upstream: fetch only main (it has about 50 work branches), never push to it.
git remote add upstream https://github.com/confluentinc/intellij.git
git remote set-branches upstream main
git remote set-url --push upstream DISABLED
git fetch upstream
# With jenv: point .java-version at a Temurin 21 that `jenv versions` lists. On the old machine it held
# "temurin64-21.0.12.1". Never commit that file.
```

A prompt for the new agent:

> This is my fork of the Confluent Kafka plugin. Check out the branch `handoff/pr-split` and read `HANDOFF.md`
> completely. Then continue with the upstream plan described there, starting with step 3. Ask me before pushing,
> force-pushing, opening pull requests or merging.

## Appendix: which branch owns what

Files that several branch commits touch, without `CHANGELOG.md` (code below `src/io/confluent/intellijplugin/`, tests
below `test/io/confluent/intellijplugin/`, other paths from the repository root). Use it to decide whether the backup
version of a file is the exact resolution.

| File                                                    | Branches                                     |
|---------------------------------------------------------|----------------------------------------------|
| `build.gradle.kts`                                      | L1, F1                                       |
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
