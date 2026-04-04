# Guidelines for AI Agents

This document defines the operational standards for AI agents working in this
repository.

## Canonical Sources and Precedence

When guidance appears in multiple files, use this order:

1. Machine-enforced configuration:
   - `.flake8`
   - `.editorconfig`
   - `setup.cfg`
   - `githooks/pre-commit`
   - `validate-file-quality.sh`
2. Canonical prose reference:
   - `CODING_STANDARDS.md`
3. Short summary for agent bootstrap:
   - `.github/copilot-instructions.md`

If a conflict is found, follow the higher-priority source and update docs to
remove the conflict.

## Rule Scope

- **79-character line length:** Python files only (`*.py`)
- **Trailing whitespace (spaces/tabs):** all code and config files
- **Markdown files:** trailing whitespace is allowed for formatting/rendering
- **Line endings:** LF only for all text files

## Required Workflow

Before editing:

1. Read `CODING_STANDARDS.md` if rule details are needed.
2. Prefer config-driven enforcement over ad-hoc rules.

After editing:

1. For Python files, run lint/validation checks used by the repo.
2. For all edited text files, run `validate-file-quality.sh <file>`.
3. Fix violations before presenting changes.

## Related Files

- `CODING_STANDARDS.md` - canonical rule definitions and validation commands
- `.flake8` - Python linting and line-length enforcement
- `.editorconfig` - editor-level consistency rules
- `setup.cfg` - centralized pytest and coverage configuration
- `githooks/pre-commit` - pre-commit enforcement entry point
- `validate-file-quality.sh` - file-level validation helper
