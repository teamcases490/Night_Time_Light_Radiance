# NTLR Pipeline

## Overview

NTLR is a production-grade, fault-tolerant, batch-based Google Earth Engine + Google Drive pipeline for extracting VIIRS Nighttime Lights metrics, historical trends, and enriched statistical features from large coordinate datasets.

It is designed for:

- Large-scale coordinate CSV processing
- Resume-safe interrupted execution
- Batch-wise GEE export
- Overlapped extraction + download + postprocess
- Real-time final CSV merging

---

# Core Features

## Interactive Terminal Configuration

When you run the pipeline:

- CSV filename input
- Longitude column input
- Latitude column input
- Batch size input

### Defaults:

- File: `location.csv`
- Longitude: `Longitude`
- Latitude: `Latitude`

---

# Smart Pipeline Workflow

## Batch Flow:

### Batch 1:

- Sent to Google Earth Engine

### Batch 2:

- Sent to GEE while:
  - Batch 1 downloads from Drive
  - Batch 1 postprocesses
  - Batch 1 appends to final CSV

### Batch N:

- Current batch exports
- Previous batch downloads + enriches simultaneously

---

# Fault Tolerance

The pipeline automatically tracks progress using:

```bash
pipeline_state.json
```

## Handles:

- WiFi interruption
- Power failure
- Script stop
- Token expiration
- Batch retry

## Resume:

If interrupted at Batch 7:

```bash
python ntlr_pipeline.py
```

Automatically resumes from:

```bash
Batch 7 onward
```

---

# Full Automated Setup

## Windows:

Run:

```bash
setup.bat
```

---

# `setup.bat` Automatically:

## Creates:

- `.venv/`
- `requirements.txt`
- `data/`
- `logs/`
- `location.csv`
- `final_ntlr_enriched.csv`

## Installs:

- pandas
- earthengine-api
- pydrive2
- numpy
- scipy
- Google auth dependencies

---

# Manual Requirements

## 1. Google Earth Engine Authentication

Run:

```bash
earthengine authenticate
```

---

## 2. Google Drive OAuth

Place:

```bash
client_secrets.json
```

in project root.

### Important:

OAuth type must be:

```bash
Desktop App
```

NOT Web App.

---

# Folder Structure

```bash
NTLR_Pipeline/
│
├── .venv/
│
├── config.py
├── pipeline_state.py
├── ntlr_extractor.py
├── mount_extracted_data_from_drive.py
├── ntlr_postprocess.py
├── ntlr_pipeline.py
│
├── setup.bat
├── requirements.txt
├── README.md
│
├── client_secrets.json
├── location.csv
│
├── token.json
├── pipeline_state.json
│
├── data/
│   ├── .gitkeep
│   ├── ntlr_batch_1.csv
│   ├── ntlr_batch_1_enriched.csv
│   └── ...
│
├── logs/
│   └── .gitkeep
│
└── final_ntlr_enriched.csv
```

---

# Running the Pipeline

## Step 1:

Activate environment:

```bash
.venv\Scripts\activate
```

---

## Step 2:

Run:

```bash
python ntlr_pipeline.py
```

---

# Input CSV Format

## Default:

```csv
Latitude,Longitude
19.0760,72.8777
18.5204,73.8567
```

---

# Custom CSV:

Any column names are supported via terminal prompts.

---

# Output Files

# Main Final Output:

```bash
final_ntlr_enriched.csv
```

This is your continuously updated master enriched dataset.

---

# Batch Files:

```bash
data/ntlr_batch_1.csv
data/ntlr_batch_1_enriched.csv
data/ntlr_batch_2.csv
```

---

# Extracted Features

## Multi-Buffer (500m, 1000m, 1500m, 2000m):

### Current (March 2025):

- mean
- median
- stdDev
- variance
- min
- max
- range
- p25
- p75
- iqr
- cv

---

# Historical:

### March Mean:

- hist_2020
- hist_2021
- hist_2022
- hist_2023
- hist_2024
- hist_2025

---

# Recommended Batch Sizes

## Small:

```bash
100–500
```

## Medium:

```bash
500–1500
```

## Large:

```bash
1500–3000
```

### Best Practical:

```bash
500–2000
```

---

# Common Commands

## Install manually:

```bash
pip install -r requirements.txt
```

---

## Activate:

```bash
.venv\Scripts\activate
```

---

## Run:

```bash
python ntlr_pipeline.py
```

---

## Reset pipeline state:

Delete:

```bash
pipeline_state.json
```

---

# Common Errors

## Error:

```bash
ModuleNotFoundError: No module named 'pandas'
```

### Fix:

```bash
pip install -r requirements.txt
```

---

## Error:

```bash
Missing client_secrets.json
```

### Fix:

Download from Google Cloud OAuth Desktop App.

---

## Error:

```bash
Earth Engine not authenticated
```

### Fix:

```bash
earthengine authenticate
```

---

# Production Notes

## Keep:

```bash
client_secrets.json
```

in root always.

---

## `data/` folder:

Stores batch CSVs

---

## `final_ntlr_enriched.csv`:

Master cumulative dataset

---

## `pipeline_state.json`:

Resume state tracker

---

# Recommended Enhancements (Future)

- Parallel multi-task exports
- Auto Drive cleanup
- Batch compression
- Integrated NTLR scoring
- Amenity + road + infrastructure fusion
- Region-adaptive weighting

---

# Security Notes

## Never share:

- `client_secrets.json`
- `token.json`

---

# Author Workflow

```bash
location.csv
→ Batch Split
→ GEE Extraction
→ Google Drive Export
→ Local Download
→ Postprocess
→ Master Merge
→ final_ntlr_enriched.csv
```

---

# Final Goal

A scalable infrastructure-grade Nighttime Lights extraction engine ready for:

- NTLR scoring
- Land valuation
- Infrastructure classification
- Hybrid economic intelligence modeling
