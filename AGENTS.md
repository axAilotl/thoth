# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work atomically
bd close <id>         # Complete work
bd dolt push          # Push beads data to remote
```

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.

**Use these forms instead:**
```bash
# Force overwrite without prompting
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file

# For recursive operations
rm -rf directory            # NOT: rm -r directory
cp -rf source dest          # NOT: cp -r source dest
```

**Other commands that may prompt:**
- `scp` - use `-o BatchMode=yes` for non-interactive
- `ssh` - use `-o BatchMode=yes` to fail instead of prompting
- `apt-get` - use `-y` flag
- `brew` - use `HOMEBREW_NO_AUTO_UPDATE=1` env var

## Coding Standards

1. Security: fail closed.
2. Error handling: no swallowing.
3. No legacy code paths or compatibility shims.
4. No silent fallbacks for required config or security-sensitive dependencies.

## Structure Requirements

- Do not create or expand god files.
- Prefer focused modules with clear ownership boundaries.
- Extend existing primitives before inventing new parallel abstractions.
- If a file is getting large, split by domain capability before adding more.
- Verify new runtime code is wired to a real entrypoint or registry path.
- Before closing work, confirm there are no unreachable production modules or unwired settings.

## Parallel Work Safety

Never delete or destroy shared git state without explicit user confirmation.

Forbidden without approval:

- `git branch -D` or `git branch -d`
- `git worktree remove`
- `git stash drop` or `git stash clear`
- `git reset --hard`
- `git checkout -- .`
- `git clean -f`
- deleting directories that may contain worktrees or active agent state

If you notice unexpected local changes that you did not create, stop and ask how to proceed.

## Parallel Execution

Use parallel streams only when the dependency graph supports it. Prefer up to three concurrent streams for multi-bead efforts; fewer is better when the ready queue is narrow or the merge surface is tightly coupled.

### Integration branch policy

- For a large multi-bead initiative, create a dedicated integration branch and keep parallel work scoped to that branch until the effort is validated.
- Create each worktree from the integration branch and merge each completed stream back into the integration branch, not directly into the protected release branch.
- Keep the release branch stable while the parallel effort is in flight. Merge the integration branch into the release branch only after the user or operator finishes the required manual verification.

### Standard parallel loop

1. Select the next highest-priority ready beads that are safe to run in parallel.
2. Create one worktree per selected bead or stream.
3. Spawn at most one sub-agent per worktree and give it explicit ownership of its bead, files, and validation scope.
4. Require each sub-agent to implement only its assigned bead, run targeted validation, and commit its work inside its own worktree.
5. Let the streams run without constant check-ins. Do not micro-manage active workers; check on them only when they report a blocker, finish, or have been silent for an unusually long interval such as 20 minutes.
6. When the selected streams are complete, merge them back into the integration branch and resolve conflicts only at the orchestrator level.
7. Run validation on the integration branch for every merged area, plus broader regression coverage when the combined change surface warrants it.
8. Update bead state after integration: close completed beads with evidence, and reopen or create follow-up beads when new work is discovered.
9. Repeat with the next set of ready beads until the non-deferred work for the initiative is complete.

### Merge and validation policy

- Merge blocker-unlocking or dependency-clearing beads first when merge order matters.
- If two streams conflict, resolve the conflict once on the integration branch and rerun the impacted tests there.
- Do not close a bead without validation evidence for the area it changed.
- If a repo-native lint command exists, run it. If no `package.json` or `npm run lint` target exists, use the repo-native lint or static checks instead of inventing an npm wrapper.
- Keep the bead tracker aligned with the integrated branch state, not with partial work still isolated in side worktrees.

## Orchestration Hygiene

If you spawn subagents or parallel workers:

1. Wait for them to finish or interrupt them intentionally.
2. Record each worker's final state before shutdown.
3. Call `close_agent` on every spawned handle.
4. Verify no intentionally active workers remain before ending the session.

A worker saying "done" is not enough. Handle cleanup is required.

## Landing The Plane

A work session is not complete until the changes are committed and pushed.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
