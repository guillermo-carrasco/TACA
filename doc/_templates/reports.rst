Delivery Reports
================

Introduction
------------
A common task within NGI is to create delivery reports. These can be general
reports *(eg. when sequencing is complete)* or analysis reports *(eg. best
practice reports)*.

This pm controller handles a common framework for generating PDF reports with
consistent styling. Each report type then has a subdirectory containing code
and assets specific to that report. **Each report type should also have its
own readme, located here**.

These report scripts should be as stupid as possible. All analysis and plotting
should be done before this script is called. Here, we just want to scoop up
results and plots from the file structure and assemble them into a PDF.

File structure
--------------
A typical structure (made up, these reports don't exist yet) is show below:

.. code-block:: bash

    .
    ├── reports.py
    ├── common
    |   ├── common_styles.rst
    |   └── assets
    |       ├── ngi_logo.png
    |       └── SciLifeLab_logo.png
    └── reports
        ├── project_summary
        |   ├── project_summary_styles.rst
        |   ├── project_summary_marco_template.rst
        |   ├── assets
        |   |   ├── tick.png
        |   |   └── cross.png
        |   └── project_summary_reports.py
        ├── IGN_pipeline_delivery_reports
        |   ├── ign_report_styles.rst
        |   ├── ign_report_marco_template.rst
    [ ... ]

Any new code that could be used in other reports should be added to the core
code. Anything that is specific to just your report should be kept separate
in your directory.
            
Documentation
-------------
As each report type will expect different files *(with strict file naming
conventions)*, good documentation is critical. You must state all file
dependencies and how these files should be formatted / generated.

Looking to the Future
---------------------
It's possible that in the future we may also offer reports in other formats,
such as HMTL. As such, if all assets can be generated in HTML-friendly formats
(ie. ``.png`` or ``.svg`` files instead of ``.pdf`` graphs), that could
save work in the future..


