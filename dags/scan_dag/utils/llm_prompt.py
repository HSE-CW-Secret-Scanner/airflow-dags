def generate_llm_prompt(input_data):
    prompt = """
**Role**: Act as a senior information security specialist performing automated secret scanning validation with full context awareness.

**Task**: Evaluate each JSON entry's secret field against these criteria:
1. **Secret Characteristics**:
   - Entropy analysis (length ≥15 chars, char diversity for high entropy)
   - Contextual validation (matches secret type expectations)
   - Exclusion of test patterns/placeholders
2. **Metadata Context**:
   - `repo_path`/`file_path` patterns (e.g., test/, examples/)
   - `snippet` context analysis
   - `rule_name` alignment with secret type
3. **Historical Risk**:
   - `author_email` domain reputation
   - `detected_at` recency consideration

**Output Requirements**:
- Strict JSON format preserving ALL original fields
- Add only these new fields:
  - `"False-positive"`: `0` (valid) or `1` (invalid)
  - `"Confidence"`: `"High"`/`"Medium"`/`"Low"`
  - `"Reason"`: Required for false positives (max 15 words)
- Never modify existing field values
- No additional commentary

**Input Example**:
[
  {
    "1": {
      "repo_path": "https://github.com/company/prod-repo",
      "commit_hash": "a1b2c3d",
      "author": "John Doe",
      "author_email": "john@company.com",
      "rule_name": "High Entropy String Detector",
      "secret": "fds1$ko90EM",
      "snippet": "const apiKey = 'fds1$ko90EM'",
      "file_path": "src/auth/config.js",
      "line": 42,
      "detected_at": "2023-05-15T14:32:11Z"
    }
  }
]

**Output Example**:
{
  "1": {
    "repo_path": "https://github.com/company/prod-repo",
    "commit_hash": "a1b2c3d",
    "author": "John Doe",
    "author_email": "john@company.com",
    "rule_name": "High Entropy String Detector",
    "secret": "fds1$ko90EM",
    "snippet": "const apiKey = 'fds1$ko90EM'",
    "file_path": "src/auth/config.js",
    "line": 42,
    "detected_at": "2023-05-15T14:32:11Z",
    "False-positive": 0,
    "Confidence": "High"
  }
}

**Validation Rules**:
||| High Confidence Valid Secrets (False-positive=0) |||
- High entropy strings (>64 bits) in production paths
- Cryptographic keys with proper prefixes (ssh-rsa, BEGIN PRIVATE KEY)
- Cloud provider patterns (AKIA[0-9A-Z]{16})

||| False-positive=1 When |||
1. File path contains: /test/, /examples/, /mocks/
2. Snippet shows test context (e.g., "example_key")
3. Author email from test domains (example.com, test.org)
4. Rule mismatch (e.g., "Keyword" rule detecting 32-char hex)

**Process this JSON**:
""" + str(input_data[0])

    return prompt