# Sphinx Documentation

This directory contains the Sphinx-based documentation system for dhtrader.

## Structure

```
docs/sphinx/
├── source/           # Source files (RST, conf.py, API)
│   ├── conf.py       # Sphinx configuration
│   ├── index.rst     # Documentation root
│   └── api/          # Auto-generated API documentation
├── build/            # Built documentation (HTML, etc.)
│   └── html/         # HTML output
├── Makefile          # Build automation (Linux/Mac)
└── make.bat          # Build automation (Windows)
```

## Building Documentation

### Using the shell script
```bash
./mkdocs.sh
```

### Using Sphinx directly
```bash
# Build HTML documentation
sphinx-build -b html docs/sphinx/source docs/sphinx/build/html

# Or use the Makefile (Linux/Mac)
cd docs/sphinx
make html

# Or use make.bat (Windows)
cd docs/sphinx
make.bat html
```

### View Documentation
Open `docs/index.html` or `docs/sphinx/build/html/index.html` in your browser.

## Requirements

Sphinx must be installed:
```bash
pip install sphinx
```

## Configuration

Edit `docs/sphinx/source/conf.py` to customize:
- Project name and metadata
- Theme and styling
- Extensions and plugins
- Autodoc settings

Edit `docs/sphinx/source/index.rst` to modify the main documentation structure.

## API Documentation

Module documentation is automatically generated from docstrings in Python files.
Edit `mkdocs.sh` to configure:
- Which folders to scan (`SCAN_FOLDERS`)
- Which files to exclude (`EXCLUDE_FILES`)
