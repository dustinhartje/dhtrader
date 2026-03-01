# Code Standards and Enforcement

This document describes the coding standards enforced in this project and how they are automatically validated.

## Standards

### Line Length Limit: 79 Characters

All Python code must adhere to the 79-character line limit as defined in PEP 8. This ensures code readability and compatibility with various development environments.

**Tools that enforce this:**
- `.flake8` configuration file (linting)
- `.editorconfig` for editor integration
- `githooks/pre-commit` hook (on commit)

**Why 79?**
- PEP 8 standard for Python
- Works well with split-screen editors
- Consistent with Python community conventions

### No Trailing Whitespace

All files must have no trailing whitespace (spaces/tabs at end of lines).

**Tools that enforce this:**
- `.editorconfig` (editor-level trimming)
- `.flake8` linting rules W291 and W293
- `githooks/pre-commit` hook

### File Endings

- All files must end with a newline character
- Unix line endings (LF, not CRLF)

## Enforcement Tools

### 1. EditorConfig (.editorconfig)

VS Code and most editors support EditorConfig for automatic code formatting rules:
- Automatically trims trailing whitespace
- Enforces line endings
- Sets proper indentation

**Install:** Most editors have built-in support or plugins readily available.

### 2. Flake8 (.flake8)

Linting tool that checks for PEP 8 compliance:
```bash
flake8 .  # Run locally before committing
```

**Key rules:**
- E501: line too long (>79 characters)
- W291: trailing whitespace
- W293: blank line contains whitespace

### 3. Pre-commit Hook (githooks/pre-commit)

Automatically runs `flake8` before each commit to catch violations:
```bash
# Install hook (from repository root):
cp githooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit

# Verify it's installed:
chmod +x .git/hooks/pre-commit && ls -l .git/hooks/pre-commit
```

## Validation Command Reference

Use these commands to validate Python files for compliance:

### Check for line length violations:
```bash
awk 'length > 79 {print NR": "length" chars: "$0}' <file>
```

### Check for trailing whitespace (spaces):
```bash
grep -n " $" <file>
```

### Check for trailing whitespace (tabs):
```bash
grep -n "\t$" <file>
```

### Check line endings:
```bash
file <file>  # Should show "LF line terminators"
```

### Verify no violations exist:
```bash
awk 'length > 79' <file> | wc -l    # Should output: 0
grep -n " $" <file> | wc -l         # Should output: 0
file <file> | grep -q "LF"           # Should succeed
```

## Making Changes

When making changes to files, ensure:

1. **Line length:** Keep lines ≤79 characters
   - Break long strings using parentheses or backslash
   - Indent continuation lines appropriately

2. **Trailing whitespace:** Never create trailing spaces
   - Use `.editorconfig`-compatible editor (VS Code, PyCharm, etc.)
   - Or use automated tools: `sed -i 's/[[:space:]]*$//' file.py`

3. **File endings:** Always end files with a newline

## Example: Fixing Long Lines

**Bad:**
```python
result = sym.market_is_open(trading_hours="eth", target_dt=f"{date_without} 16:15:00", check_closed_events=False)
```

**Good (wrapped with parentheses):**
```python
result = sym.market_is_open(
    trading_hours="eth",
    target_dt=f"{date_without} 16:15:00",
    check_closed_events=False
)
```

## Continuous Integration

The checks run automatically:
- **On commit:** Pre-commit hook validates before allowing commit
- **On push:** CI/CD pipeline validates all changes
- **On PR:** Automated checks prevent merge if standards violated

## Configuration Files Reference

- **`.flake8`**: Flake8 linting configuration with max-line-length=79
  - Primary enforcement mechanism for CI/CD
  - Also used by pre-commit hooks

- **`.editorconfig`**: Editor-level code formatting rules
  - Mirrors `.flake8` settings for real-time editor feedback
  - Automatically trims trailing whitespace
  - Supports VS Code, PyCharm, and other editors

- **`setup.cfg`**: Centralized project configuration
  - `[metadata]`: Package information
  - `[options]`: Python version and dependencies
  - `[tool:pytest]`: Pytest test discovery and markers
  - `[coverage:*]`: Code coverage configuration
  - Note: Pytest config consolidated here (no pytest.ini file)

- **`githooks/pre-commit`**: Git pre-commit hook script
  - Runs flake8 before allowing commits
  - Prevents violations from entering repository
