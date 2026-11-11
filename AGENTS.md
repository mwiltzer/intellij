# Agent instructions

This checkout is on `handoff/pr-split`, a handoff branch of the fork `mwiltzer/intellij`.

1. Read [HANDOFF.md](HANDOFF.md) first and completely. It holds what the previous agent session knew: the branches that
   are meant to become pull requests, the upstream plan (upstream first, the fork as fallback), the merge order, the
   expected conflicts and how to resolve them, how to verify, and the open backlog.
2. Follow the project conventions in [CLAUDE.md](CLAUDE.md), [.github/copilot-instructions.md](.github/copilot-instructions.md)
   and [AI_POLICY.md](AI_POLICY.md).
3. Never open a pull request from `handoff/pr-split` or `backup/all-optimizations`, never merge them, and never base new
   work on them.
4. Ask the user before pushing, force-pushing, opening pull requests or merging.
