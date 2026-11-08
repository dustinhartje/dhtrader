# Python Code Standards
- Maximum line length: 79 characters (Python files only)
- No trailing whitespace in code and config files
- Markdown files may use trailing whitespace when needed for formatting
- Blank lines must contain no whitespace characters
- Use implicit line continuation within parentheses/brackets
- Follow PEP 8 style guide
- See [AGENTS.md](../AGENTS.md) and
	[CODING_STANDARDS.md](../CODING_STANDARDS.md) for full rules,
	validation commands, and agent checklists

# Naming
- Include concrete domain object types, not just role qualifiers, in new or
	renamed symbols. Follow
	[Domain object naming](../CODING_STANDARDS.md#domain-object-naming).
- Name timing starts `time_<stage>` and assigned elapsed durations
	`elapsed_<stage>` (private attributes: `_time_` / `_elapsed_`). Keep inline
	elapsed calculations; do not introduce assignments solely for naming. Follow
	[Timing pattern](../CODING_STANDARDS.md#timing-pattern).

# Documentation
- Follow [Documentation ownership](../CODING_STANDARDS.md#documentation-ownership)
	for function/method docstrings and related inline comments.

# Running scripts
- Use `python3` to run Python scripts
- Use `/env/bin/activate` to activate the virtual environment when appropriate

# Agent tools
- Never use Pylance or Pylance MCP tools, including through skills.
	Use direct file inspection, repository tools, and runtime tests instead.

# File editing
- Always prefer editor/file-edit tools over terminal tools for changes,
	including bulk mechanical edits, even if a terminal command is shorter.
- Prefer per-change Keep/Undo review, then per-file Keep/Undo, then diffs
	or Source Control. Use per-change Keep/Undo whenever available, including
	when reintroduced in a future update. Missing Keep/Undo controls do not
	block editing with available file-edit tools.
- Modify files with terminal tools only when available editor/file-edit
	tools cannot make the change. In interactive chat, explain why terminal
	tools are necessary, describe the proposed change, and request operator
	permission. Wait for explicit approval before proceeding.
- This rule includes terminal writes and file-rewriting scripts (`sed -i`,
	`perl -pi`, `awk`, `>`/`>>` redirection, `tee`, terminal `patch`, etc.).
- Use terminal tools for read-only inspection, tests, linters, and
	validation scripts; commands that modify files follow the rule above.
