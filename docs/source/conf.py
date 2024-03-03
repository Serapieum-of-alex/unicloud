# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
# import pydata_sphinx_theme

import os
import sys

# for the auto documentation to work
sys.path.insert(0, os.path.abspath("../../src/unicloud"))


project = "unicloud"
copyright = "2024, Mostafa Farrag"
author = "Mostafa Farrag"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
]

templates_path = ["_templates"]
exclude_patterns = []

root_doc = "index"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"

# Set the theme name
# Optionally, you can customize the theme's configuration
html_theme_options = {
    "navigation_depth": 4,
    "show_prev_next": False,
    "show_toc_level": 2,
    # Add any other theme options here
}

html_static_path = ["_static"]
