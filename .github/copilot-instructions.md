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

# Running scripts
- Use `python3` to run Python scripts
- Use `/env/bin/activate` to activate the virtual environment when appropriate

# Agent tools
- Never use Pylance or Pylance MCP tools, including through skills.
	Use direct file inspection, repository tools, and runtime tests instead.

# File editing
- When running in VS Code, use VS Code's editor file-edit functions for
	every change so the user can inspect and accept/reject the editor diff.
	If these are unavailable, stop and tell the user; do not use terminal
	writes or scripts that rewrite files as a fallback.
- Never modify files with terminal commands (`sed -i`, `perl -pi`, `awk`,
	`>`/`>>` redirection, `tee`, `patch`, etc.)
- Always use the editor's file-edit tools, even for bulk mechanical edits
	such as project-wide renames, so every change appears in the chat
	review UI with keep/undo per file
- Terminal use is limited to read-only inspection, running tests, linters,
	and validation scripts
