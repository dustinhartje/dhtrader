# Guidelines for AI Agents

This document contains essential guidelines for AI agents (including GitHub
Copilot and other automated assistants) working on this project.

## Python File Editing Requirements

When editing Python files (`.py`), all changes MUST adhere to three core
rules.

**For complete rule definitions, examples, tools, and configuration,
see [CODING_STANDARDS.md](CODING_STANDARDS.md).**

### Rule 1: Maximum Line Length - 79 Characters

- All lines in Python files must not exceed 79 characters
- Complies with PEP 8 standard
- Use parentheses to break long strings across multiple lines

### Rule 2: No Trailing Whitespace

- Python files must have zero trailing spaces or tabs
- Blank lines should be completely empty (zero characters)

### Rule 3: Unix Line Endings (LF Only)

- All files must use LF line endings, not CRLF

## Pre-Edit Checklist (Python Files Only)

Before making any changes to a Python file:

1. Review rules above and [CODING_STANDARDS.md](CODING_STANDARDS.md)
2. Run pre-edit validation using commands from
   [CODING_STANDARDS.md section "Validation Command
   Reference"](CODING_STANDARDS.md#validation-command-reference)
3. Plan line breaks to stay within 79 characters

## Post-Edit Verification (Python Files Only)

After editing a Python file, run the verification commands from
[CODING_STANDARDS.md section "Validation Command
Reference"](CODING_STANDARDS.md#validation-command-reference)

If any violations are found, fix them immediately before presenting changes.

## Project Configuration Files

- **`.flake8`** - Flake8 linting configuration (canonical enforcement)
- **`.editorconfig`** - Editor-level formatting rules
- **`CODING_STANDARDS.md`** - Complete standards and validation commands
- **`.agent-edit-guidelines.md`** - Detailed agent checklists and examples
- **`validate-file-quality.sh`** - Automated validation script

## Scope of Rules

- **Python files ONLY** (.py, tests, scripts)
- Configuration files, markdown, shell scripts not subject to 79-char limit
- Trailing whitespace rules apply ONLY to Python files
- Line ending (LF) requirement applies to all files

## See Also

- [README.md](README.md) - Project overview and agent guidelines summary
- [CODING_STANDARDS.md](CODING_STANDARDS.md) - **Canonical reference for all
  rules, examples, validation commands, and tools**
- [.agent-edit-guidelines.md](.agent-edit-guidelines.md) - Detailed
  agent-specific checklists with code examples

## Key Project Information

See the main [README.md](README.md) for:
- Code quality standards overview
- Market era analysis details
- Project structure and design principles

See [CODING_STANDARDS.md](CODING_STANDARDS.md) for:
- Detailed examples and patterns
- Exception handling procedures
- Comprehensive reference material
