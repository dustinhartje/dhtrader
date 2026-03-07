# Configuration file for the Sphinx documentation builder.

import os
import sys

# Add package parent to path for autodoc imports (dhtrader.<module>)
PACKAGE_PARENT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../..')
)
sys.path.insert(0, PACKAGE_PARENT)

# -- Project information
project = 'dhtrader'
copyright = '2026'
author = 'Development Team'
release = '1.0'

# -- General configuration
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
]

# Autosummary settings
autosummary_generate = True

# Napoleon settings for Google/NumPy style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True

# Autodoc settings
autodoc_member_order = 'bysource'
autodoc_typehints = 'description'
autodoc_preserve_defaults = True
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
}

# Cleaner TOC in sidebar
toc_object_entries_show_parents = 'hide'

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output
html_theme = 'alabaster'
html_theme_options = {
    'github_user': '',
    'github_repo': '',
    'github_button': False,
    'page_width': '1200px',
    'sidebar_width': '250px',
}

# Sidebar configuration - add local TOC to show page contents
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'localtoc.html',  # Shows classes/functions on current page
        'relations.html',
        'searchbox.html',
    ]
}

# Custom CSS files
html_static_path = ['_static']
html_css_files = [
    'custom_responsive.css',
    'custom_doc_styling.css',
]
html_js_files = [
    'collapsible_toc.js',
    'module_docstring.js',
]

# Sphinx output format options
html_show_sourcelink = True
html_show_sphinx = True
html_show_copyright = True

# Intersphinx mapping (optional - for linking to other projects)
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
}
