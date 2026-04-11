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

## New Data Class Checklist

When creating any new data class (one that is stored, retrieved,
or serialized), verify each item before presenting the code:

- [ ] `to_json()` uses `deepcopy(self.__dict__)` as the base; only
      adds inline conversions for fields that require normalization.
- [ ] `to_clean_dict()` is simply `json.loads(self.to_json())`.
- [ ] `from_dict()` uses `d["key"]` direct access (no `.get()` with
      silent defaults) for every field.
- [ ] `uniq_id` generated via `new_uuid()` from `dhcommon`;
      all other id fields derived from it, not generated separately.
- [ ] `*_id_short` preserves the full human-readable prefix and
      replaces only the uuid suffix with its last 8 hex chars.
- [ ] `created_epoch` (int) and `created_dt` (ISO string) both
      present; one derived from the other.
- [ ] `_EQ_FIELDS` frozenset declared as a class variable listing
      every attribute compared by `__eq__`.
- [ ] `_EQ_EXCLUDE` frozenset declared as a class variable listing
      every attribute intentionally excluded from `__eq__`, with an
      inline comment per entry explaining why (derived, config, etc.).
- [ ] `__eq__` uses `all(getattr(self, f) == getattr(other, f) for f
      in self._EQ_FIELDS)` — not hand-rolled attribute chains.
- [ ] `_EQ_FIELDS | _EQ_EXCLUDE` exactly covers `instance.__dict__`
      with no gaps and no phantom entries.
- [ ] Unit tests cover: construction defaults, all-fields construction,
      `to_json` returns valid JSON, `to_clean_dict` matches parsed
      `to_json`, mutation of the returned dict does not affect the
      instance, and `from_dict` round-trip.
- [ ] A `test_ClassName_eq_covers_all_attributes()` test exists that
      accepts `assert_eq_fields_cover_instance` as a fixture parameter
      and calls it with the instance.
- [ ] A `test_ClassName_eq_field_sensitivity()` test exists that
      accepts `run_eq_field_sensitivity` as a fixture parameter and
      verifies: (1) a deepcopy of the instance equals the original;
      (2) setting each `_EQ_FIELDS` member to the sentinel causes
      inequality; (3) setting each truly-excluded `_EQ_EXCLUDE` member
      to the sentinel does not affect equality.  For classes using
      `sub_eq()`, `parameters` is filtered from the truly-excluded loop
      and tested separately to confirm it does cause inequality.

## Related Files

- `CODING_STANDARDS.md` - canonical rule definitions and validation commands
- `.flake8` - Python linting and line-length enforcement
- `.editorconfig` - editor-level consistency rules
- `setup.cfg` - centralized pytest and coverage configuration
- `githooks/pre-commit` - pre-commit enforcement entry point
- `validate-file-quality.sh` - file-level validation helper

## Logging and Comments

- **`log.critical` before every `raise`**: include function
  name and the triggering value; applies whether the
  exception exits the script or propagates to a caller.
- **`log.error`** for non-raising error conditions only
  (unexpected states logged but execution continues).
- **`log.info`/`log_say` entry and exit** on every public
  store/retrieve/delete/list/review function; `log_say` for
  console-visible messages, `log.info` for file-only.
- **`log.debug`** for timing (`log_timer()`), per-item loop
  detail, and test-mode overrides — not visible in normal
  operation.
- **Timing**: `time_x = tpc()` before, `log_timer(label,
  tpc() - time_x, ...)` after each meaningful stage.
- **Inline comments** describe intent, not restatement of
  code; required before every non-obvious block.
- **No logging in unit tests.**
