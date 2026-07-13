# Workforce Analysis Template

A reusable Python template for occupational workforce analysis, developed from the methods used in the Radius supply-demand modeling pipeline.

The template walks through the core analytical steps: pulling and cleaning occupational data from BLS and Lightcast sources, calculating supply and demand estimates, projecting shortage or surplus trajectories, and generating output tables and charts. It exists as both a `.py` script and a Jupyter notebook for interactive exploration.

Intended as a starting point for new occupation-specific analyses rather than a standalone application.

## Contents

- `workforce_analysis_template.ipynb` -- notebook version with inline outputs
- `workforce_analysis_template.py` -- script version for pipeline integration

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
