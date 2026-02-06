# GitHub Issue Templates

Standard issue templates for Franklin's repos (causaganha, baliza, egregora).

## Files

- **bug_report.md** - Report bugs or unexpected behavior
- **feature_request.md** - Suggest new features or enhancements
- **documentation.md** - Report documentation issues
- **config.yml** - Configure issue template picker

## Installation

Copy these templates to each repo:

```bash
# For causaganha
cp -r .github/ISSUE_TEMPLATE /path/to/causaganha/.github/

# For baliza
cp -r .github/ISSUE_TEMPLATE /path/to/baliza/.github/

# For egregora
cp -r .github/ISSUE_TEMPLATE /path/to/egregora/.github/
```

Then commit and push to each repo.

## Customization

Edit `config.yml` to adjust contact links for each repo (update URLs).

---

**Created:** 2026-02-06 by Funes  
**Purpose:** Standardize issue reporting across Franklin's projects
