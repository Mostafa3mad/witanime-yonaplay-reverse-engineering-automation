<div align="center">

# Series Automation

### From Browser Workflow Analysis to Python Automation

<p>
  <img src="https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.13">
  <img src="https://img.shields.io/badge/Requests-HTTP%20Automation-2EA44F?style=for-the-badge" alt="Requests">
  <img src="https://img.shields.io/badge/AES--GCM-Cryptography-6F42C1?style=for-the-badge" alt="AES-GCM">
  <img src="https://img.shields.io/badge/Concurrency-ThreadPoolExecutor-FF9800?style=for-the-badge" alt="Concurrency">
  <img src="https://img.shields.io/badge/JSON-Processing-000000?style=for-the-badge&logo=json&logoColor=white" alt="JSON">
</p>

<p>
  <strong>A Python automation toolkit for processing series metadata, episode data, server information, encrypted API responses, and JSON files.</strong>
</p>

</div>

---

## Overview

**Series Automation** is a collection of Python scripts built to turn a browser-dependent workflow into a repeatable automation pipeline.

What started as a simple automation task became a deeper technical investigation involving:

- JavaScript analysis
- Request-flow tracing
- Session and cookie handling
- Dynamic codes and keys
- Server-specific tokens
- Encrypted API responses
- AES-GCM decryption
- Response preprocessing
- Automatic retry
- Concurrent episode processing
- JSON transformation

The final goal is simple:

> **Observe → Understand → Reproduce → Automate**

---

## The Journey

```text
┌──────────────────┐
│   Browser Flow   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ JavaScript       │
│ Analysis         │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Network Request  │
│ Tracing          │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Session / Cookies│
│ Dynamic Values   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Server Tokens    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Encrypted API    │
│ Response         │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ AES-GCM          │
│ Decryption       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Data             │
│ Normalization    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ JSON Automation  │
└──────────────────┘
```

---

## Why This Project?

The interesting part wasn't simply sending an HTTP request.

The interesting part was understanding **why the request worked**.

The workflow was not:

```text
URL → Request → Response
```

It was a chain of dependent operations:

```text
Embed
  ↓
Session
  ↓
Code
  ↓
Page Key
  ↓
Sources
  ↓
Server Token
  ↓
API Request
  ↓
Encrypted `d`
  ↓
Decryption
  ↓
Final Server Data
```

Each stage provides information required by the next stage.

---

# Technical Workflow

## 01 — Open Embed

The script starts with a persistent `requests.Session()`.

This allows cookies and session state to be maintained throughout the complete workflow.

```python
session = requests.Session()
```

---

## 02 — Initialize Session

The initialization request generates dynamic session-related values.

The important values include:

```text
Code
Page Key
Session Token
```

These values are then used by subsequent requests.

---

## 03 — Discover Servers

The sources request returns the available server information.

Conceptually:

```text
Quality
   │
   ├── Mega
   │      └── Token
   │
   └── 4shared
          └── Token
```

The script extracts the server names and tokens and processes them individually.

---

## 04 — API Processing

For every server, the required values are combined:

```text
Code
+
Server Token
+
Page Key
      │
      ▼
   API Request
      │
      ▼
Encrypted `d`
```

The raw response is not immediately written to the JSON.

It first goes through preprocessing and decryption.

---

# Encryption Pipeline

The response contains an encrypted `d` value.

The processing pipeline is:

```text
Base64
   │
   ▼
Raw Bytes
   │
   ├── IV
   ├── Authentication Tag
   └── Ciphertext
          │
          ▼
     SHA-256 Key
          │
          ▼
       AES-GCM
          │
          ▼
       Plaintext
          │
          ▼
      UTF-8 Data
```

Implemented through:

```python
decryptAES()
```

Using:

```python
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
```

---

# Response Preprocessing

The raw API response is transformed before being stored.

```text
Raw Response
     │
     ▼
Extract `d`
     │
     ▼
Decrypt
     │
     ▼
Parse Result
     │
     ▼
Normalize
     │
     ▼
Store Required Value
```

This keeps the resulting `info.json` clean and predictable.

---

# Before → After

### Before

```json
{
    "servers": {
        "yonaplay": "https://mid.yonaplay.net/embed/..."
    }
}
```

### After

```json
{
    "servers": {
        "Mega": "...",
        "4shared": "..."
    }
}
```

The original Yonaplay entry is removed after successful processing and replaced with the resulting server data.

---

# Retry Architecture

Network automation is never guaranteed to succeed on the first attempt.

The project therefore uses multiple levels of retry handling.

### HTTP-Level Retry

The HTTP layer can retry selected failures using:

```text
urllib3.Retry
      +
HTTPAdapter
      +
Backoff
```

Handled status codes include:

```text
404
429
500
502
503
504
```

### Episode-Level Retry

If an episode fails during processing, the script restarts the **complete episode flow**.

```text
Attempt 1
   ↓
Open Embed
   ↓
Init Session
   ↓
Get Sources
   ↓
Get Tokens
   ↓
API
   ↓
ERROR
   │
   ▼
Attempt 2
   ↓
Open Embed Again
   ↓
Init Session Again
   ↓
Get Sources Again
   ↓
Get Tokens Again
   ↓
API Again
```

This matters because session-dependent values may need to be regenerated.

---

# Concurrent Processing

The project supports concurrent episode processing using:

```python
ThreadPoolExecutor
```

Example configuration:

```python
MAX_THREADS = 20
```

Architecture:

```text
                 ┌── Episode 1
                 │
                 ├── Episode 2
                 │
Thread Pool ─────┼── Episode 3
                 │
                 ├── Episode 4
                 │
                 └── Episode N
```

Each episode is treated as an independent unit.

A failure in one episode does not stop the remaining episodes.

---

# JSON Processing

The scripts recursively search the configured source directory for:

```text
info.json
```

The source files are kept untouched.

```text
SOURCE
   │
   ▼
Processing
   │
   ▼
OUTPUT
```

JSON is written using:

```python
json.dump(
    data,
    file,
    ensure_ascii=False,
    indent=4
)
```

This preserves Unicode content and produces readable JSON.

---

# Engineering Highlights

<table>
<tr>
<th>Area</th>
<th>Implementation</th>
</tr>

<tr>
<td><strong>HTTP Automation</strong></td>
<td><code>requests</code></td>
</tr>

<tr>
<td><strong>Session Management</strong></td>
<td><code>requests.Session</code></td>
</tr>

<tr>
<td><strong>JavaScript Analysis</strong></td>
<td>Minified / obfuscated JS investigation</td>
</tr>

<tr>
<td><strong>Request Analysis</strong></td>
<td>Network-flow tracing</td>
</tr>

<tr>
<td><strong>Token Handling</strong></td>
<td>Dynamic server tokens</td>
</tr>

<tr>
<td><strong>Cryptography</strong></td>
<td>AES-GCM + SHA-256</td>
</tr>

<tr>
<td><strong>Response Processing</strong></td>
<td>Custom preprocessing and normalization</td>
</tr>

<tr>
<td><strong>Reliability</strong></td>
<td>HTTP retry + full episode retry</td>
</tr>

<tr>
<td><strong>Concurrency</strong></td>
<td><code>ThreadPoolExecutor</code></td>
</tr>

<tr>
<td><strong>Data Processing</strong></td>
<td>JSON transformation</td>
</tr>

</table>

---

# Project Structure

```text
Series Automation/
│
├── scripts_automation_series/
│   │
│   ├── scrape_yonaplay_net.py
│   ├── for_yonaplay_mid.py
│   └── ...
│
├── analysis/
│   ├── request-flow.md
│   └── encryption-flow.md
│
├── examples/
│   ├── input.json
│   └── output.json
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

# Requirements

### Python

```text
Python 3.13
```

### Dependencies

```bash
pip install requests cryptography
```

Additional dependencies used by individual scripts should be added to:

```text
requirements.txt
```

---

# Configuration

Configure the source and destination directories:

```python
SOURCE_DIR = r"..."
DEST_DIR = r"..."
```

Configure concurrency:

```python
MAX_THREADS = 20
```

---

# Running

From the project directory:

```bash
python for_yonaplay_mid.py
```

The pipeline will:

```text
Discover Files
      ↓
Read Episodes
      ↓
Find Server URL
      ↓
Process Request Flow
      ↓
Decrypt Response
      ↓
Extract Server Data
      ↓
Replace Server Entry
      ↓
Write JSON
      ↓
Generate Summary
```

---

# Example Output

```text
================================================================================
YONAPLAY MID SERVER REPLACER
================================================================================

Source  : ...
Output  : ...
Threads : 20

Found X info.json files

================================================================================
[1/X] Series Name
================================================================================

[TRY] Episode 1 | Attempt 1/5

[OK] Episode 1
Code: ...
Servers: Mega, 4shared

[TRY] Episode 2 | Attempt 1/5

[ERROR] Episode 2
Retrying...

[TRY] Episode 2 | Attempt 2/5

[OK] Episode 2
Code: ...
Servers: Mega, 4shared

================================================================================
DONE
================================================================================

info.json files   : X
Files modified    : X
Episodes replaced : X
Errors            : X
Threads            : 20
```

---

# Engineering Challenges

The hardest part of the project was not writing the Python requests.

It was understanding the system behind them.

### Main challenges

- Analyzing heavily minified and obfuscated JavaScript.
- Mapping the complete browser request flow.
- Understanding dependencies between requests.
- Tracking dynamic session values.
- Handling cookies and session state.
- Understanding server-specific tokens.
- Processing encrypted API responses.
- Implementing AES-GCM decryption.
- Preprocessing raw responses.
- Handling transient HTTP failures.
- Restarting the complete episode flow after errors.
- Processing multiple episodes concurrently.
- Keeping source data untouched.
- Producing predictable JSON output.

---

# What This Project Represents

This project combines several areas of practical engineering:

```text
Reverse Engineering
        +
HTTP Automation
        +
Cryptography
        +
Data Processing
        +
Concurrency
        +
Reliability Engineering
        ↓
Python Automation
```

It is a practical example of taking an undocumented browser-side workflow and turning it into a structured automation pipeline.

---

# Lessons Learned

The biggest lesson from this project:

> **Don't start by asking "How do I send the request?"**
>
> **Start by asking "What is the application doing before it sends the request?"**

Once the complete flow is understood, implementation becomes much easier.

```text
Observe
   ↓
Analyze
   ↓
Trace
   ↓
Understand
   ↓
Reproduce
   ↓
Automate
   ↓
Optimize
```

---

# Responsible Use

This repository is intended for authorized automation, research, and data-processing purposes.

Do not use the scripts to:

- bypass authentication or access controls,
- circumvent security mechanisms,
- evade service restrictions,
- access content without authorization,
- intentionally overload remote services.

Always ensure that you have the appropriate authorization and comply with applicable service terms and laws.

---

<div align="center">

## The Takeaway

**The interesting part wasn't sending the request.**

**The interesting part was understanding why the request worked.**

<br>

<sub>Built as a practical exploration of request-flow analysis, cryptography, automation, and reliable data processing.</sub>

</div>

---

## License

No license has been selected yet.

If this repository is intended for public distribution, add an appropriate open-source license before publishing.
