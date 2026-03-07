#!/bin/bash

# Folders to scan for Python files
SCAN_FOLDERS=(".")

# Files to exclude (basenames without .py extension)
EXCLUDE_FILES=()

# Sphinx configuration
SOURCE_DIR="docs/sphinx/source"
BUILD_DIR="docs/sphinx/build"
MODULE_PREFIX="dhtrader"

# Create necessary directories
echo "Setting up Sphinx documentation structure..."
mkdir -p "$SOURCE_DIR"
mkdir -p "$BUILD_DIR"

# Clear build directory (keeping source files)
echo "Cleaning build directory: $BUILD_DIR"
rm -rf "$BUILD_DIR/html"
mkdir -p "$BUILD_DIR/html"

# Array to track generated modules
declare -a MODULES=()

echo "Scanning Python files..."

# Process each folder
for folder in "${SCAN_FOLDERS[@]}"; do
    if [ ! -d "$folder" ]; then
        echo "Skipping non-existent folder: $folder"
        continue
    fi

    echo "Scanning folder: $folder"

    # Find all .py files in configured folders
    while IFS= read -r -d '' pyfile; do
        # Get basename without extension
        basename=$(basename "$pyfile" .py)

        # Skip __init__ files
        if [ "$basename" = "__init__" ]; then
            continue
        fi

        # Check if file should be excluded
        skip=0
        for excluded in "${EXCLUDE_FILES[@]}"; do
            if [ "$basename" = "$excluded" ]; then
                skip=1
                break
            fi
        done

        if [ $skip -eq 1 ]; then
            echo "  Skipping excluded file: $pyfile"
            continue
        fi

        # Convert file path to module name
        if [ "$folder" = "." ]; then
            # Root-level files are part of the dhtrader package
            module_name="${MODULE_PREFIX}.${basename}"
        else
            # Files in subdirectories
            relative_path="${pyfile#$folder/}"
            module_name="${folder}.${relative_path%.py}"
            module_name="${module_name//\//.}"
        fi

        echo "  Found module: $module_name"
        MODULES+=("$module_name")

    done < <(find "$folder" -maxdepth 1 -name "*.py" -type f -print0)
done

# Sort modules alphabetically
IFS=$'\n' MODULES=($(sort <<<"${MODULES[*]}"))
unset IFS

# Generate API documentation RST files
echo "Generating API documentation RST files..."
API_DIR="$SOURCE_DIR/api"
rm -rf "$API_DIR"
mkdir -p "$API_DIR"

# Create __init__.rst for API section
cat > "$API_DIR/__init__.rst" << 'EOF'
API Reference
=============

This section contains documentation for all Python modules in the project.

.. toctree::
   :maxdepth: 2

EOF

for module in "${MODULES[@]}"; do
    echo "   $module" >> "$API_DIR/__init__.rst"

    # Create RST file for each module using sphinx-autodoc
    MODULE_FILE="$API_DIR/${module}.rst"
    cat > "$MODULE_FILE" << EOF
$(echo "$module" | tr '.' ' ')
$(printf '=%.0s' $(seq 1 ${#module}))

.. currentmodule:: $module

.. automodule:: $module
   :members:
   :undoc-members:
   :show-inheritance:
EOF
done

echo "Building HTML documentation with Sphinx..."

# Run sphinx-build
sphinx-build -b html "$SOURCE_DIR" "$BUILD_DIR/html"

if [ $? -eq 0 ]; then
    # Create a top-level docs redirect for easier discovery on GitHub.
    ROOT_INDEX="docs/index.html"
    cat > "$ROOT_INDEX" << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="0; url=sphinx/build/html/index.html">
    <title>Documentation Redirect</title>
</head>
<body>
    <p>
        Redirecting to
        <a href="sphinx/build/html/index.html">Sphinx documentation</a>.
    </p>
</body>
</html>
EOF

    echo "Adding generated documentation to git..."
    # Stage all generated docs artifacts under API + HTML output folders.
    # This includes new, changed, and removed files, while honoring
    # .gitignore exclusions (for example .doctrees and .buildinfo).
    git add -A "$API_DIR" "$BUILD_DIR/html"
    git add -A "$SOURCE_DIR/_static"
    git add "$ROOT_INDEX"

    echo "Documentation generation complete!"
    echo "View documentation at: $BUILD_DIR/html/index.html"
else
    echo "Error: Sphinx build failed"
    exit 1
fi
