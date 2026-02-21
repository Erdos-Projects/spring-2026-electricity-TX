# Git Terminal Pipeline (Beginner-Friendly)

Use this as a step-by-step pipeline from starting work to merging safely.

## 1) Plain-Language Mental Model

Think of Git like shared writing with version history:
- Your laptop has a copy of the project.
- GitHub has the shared team copy.
- A branch is your private draft line of work.
- A commit is a saved checkpoint with a message.
- Push sends your checkpoints to GitHub.
- Merge combines your work into the main shared version.

## 2) One-Time Setup

Install Git first (if not installed):

macOS (Homebrew):

```bash
brew install git
```

Ubuntu/Debian:

```bash
sudo apt update && sudo apt install -y git
```

Fedora:

```bash
sudo dnf install -y git
```

Windows (PowerShell + winget):

```powershell
winget install --id Git.Git -e
```

Confirm install:

```bash
git --version
```

Then run once on your machine:

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

Check setup:

```bash
git config --global --get user.name
git config --global --get user.email
```

## 3) Daily Pipeline (Recommended)

### Step A: Open project and sync first

Why: start from newest shared version so you do not build on old work.

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
git checkout main
git pull origin main
```

### Step B: Create a work branch

Why: keeps your changes separate and safe.

```bash
git checkout -b feature/<short-topic>
```

### Step C: Edit files

Make your changes in editor/notebook.

Check what changed:

```bash
git status
git diff
```

### Step D: Stage files

Why: choose exactly what goes into this checkpoint.

```bash
git add path/to/file1 path/to/file2
```

Review staged content:

```bash
git diff --staged
```

### Step E: Commit

Why: save a meaningful checkpoint.

```bash
git commit -m "Short clear summary"
```

### Step F: Push

Why: back up your branch and share it.

```bash
git push -u origin feature/<short-topic>
```

### Step G: Merge to main (usually via Pull Request)

Why: review first, then merge cleanly.

1. Open Pull Request on GitHub from `feature/<short-topic>` to `main`.
2. Merge in GitHub UI.
3. Update local main:

```bash
git checkout main
git pull origin main
```

## 4) Pull Before Push: Two Choices

When your branch is behind remote, you need to pull updates before push.

### Choice 1: `git pull` (merge pull)

Command:

```bash
git pull origin <branch>
```

What it does in plain words:
- Combines remote updates and your local updates with a merge commit.

Pros:
- Safer for beginners.
- Keeps full history exactly as it happened.
- Less chance of confusion when many people collaborate.

Cons:
- History becomes more cluttered (extra merge commits).
- Log can look noisy.

### Choice 2: `git pull --rebase` (rebase pull)

Command:

```bash
git pull --rebase origin <branch>
```

What it does in plain words:
- Replays your local commits on top of newest remote commits, making history look straight.

Pros:
- Cleaner, linear history.
- Easier to read timeline later.

Cons:
- Slightly harder for beginners.
- Conflict resolution can feel more complex.
- Do not rebase shared commits that others may already depend on.

Practical recommendation:
- If unsure, use regular `git pull`.
- Use `--rebase` only when you are comfortable with conflict resolution.

## 5) Merge to Main: Options and Tradeoffs

### Option A: Merge Commit (GitHub “Create a merge commit”)

Best for: preserving full context of how work was combined.

Pros:
- Shows exact branch structure.
- Safer and transparent for teams.

Cons:
- History can become busy.

### Option B: Squash and Merge

Best for: many small commits in one PR.

Pros:
- Main branch stays clean (one commit per PR).
- Easy to scan release history.

Cons:
- Individual commit details inside the branch are collapsed.
- Harder to see step-by-step evolution later.

### Option C: Rebase and Merge

Best for: clean linear history with preserved individual commits.

Pros:
- Linear timeline.
- Keeps commit granularity.

Cons:
- Commit SHAs are rewritten.
- Can confuse beginners if they track old hashes.

### Option D: Direct Push to `main`

Best for: very small solo projects or urgent hotfix by trusted maintainer.

Pros:
- Fastest path.

Cons:
- No review gate.
- Higher risk of breaking shared branch.
- Harder team coordination.

Recommendation for this project:
- Use Pull Requests.
- Prefer Squash or Merge Commit (team preference).
- Avoid direct pushes to `main` unless truly necessary.

## 6) Conflict Fix Pipeline

If pull/merge says conflict:

1. See conflicted files:

```bash
git status
```

2. Open files and resolve conflict markers.
3. Stage resolved files:

```bash
git add <resolved-files>
```

4. Finish operation:

For merge pull:

```bash
git commit
```

For rebase pull:

```bash
git rebase --continue
```

5. Push:

```bash
git push origin <branch>
```

If rebase becomes messy and you want to stop:

```bash
git rebase --abort
```

### Concrete conflict example (`test` branch + `test.csv`)

When Git reports a conflict after `git pull origin test`:

```bash
git status
# open test.csv and resolve <<<<<<< ======= >>>>>>> markers
git add test.csv
git commit
git push origin test
```

When Git reports a conflict after `git pull --rebase origin test`:

```bash
git status
# open test.csv and resolve <<<<<<< ======= >>>>>>> markers
git add test.csv
git rebase --continue
git push origin test
```

If rebase gets confusing and you want to cancel:

```bash
git rebase --abort
```

## 7) Copy-Paste Command Collections

### Safe branch workflow (recommended)

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
git checkout main
git pull origin main
git checkout -b feature/<short-topic>
# edit files
git add path/to/file1 path/to/file2
git commit -m "Short clear summary"
git push -u origin feature/<short-topic>
# create + merge PR on GitHub
git checkout main
git pull origin main
```

### Concrete example pipeline (`test` branch, update `test.csv`)

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
git checkout main
git pull origin main
git checkout -b test
# edit test.csv
git status
git diff
git add test.csv
git diff --staged
git commit -m "test"
git push -u origin test
# open PR: test -> main, merge on GitHub
git checkout main
git pull origin main
```

### Quick direct-main workflow (not preferred)

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
git checkout main
git pull origin main
# edit files
git add path/to/file1 path/to/file2
git commit -m "Short clear summary"
git push origin main
```

## 8) Daily Health Commands

```bash
git status
git branch --show-current
git log --oneline -n 10
git remote -v
```

## 9) Use AI Agents in Terminal (Codex + Gemini)

This section gives simple, copy-paste steps for both tools.

### A) Codex in Terminal (with ChatGPT sign-in)

Install Codex CLI:

```bash
# Option 1: npm
npm install -g @openai/codex

# Option 2: Homebrew (macOS)
brew install --cask codex
```

Start Codex:

```bash
codex
```

Then follow the on-screen login flow and choose **Sign in with ChatGPT**.

Optional explicit login command:

```bash
codex login
```

Check login status:

```bash
codex login status
```

Basic usage:

```bash
# Interactive mode
codex

# Non-interactive one-shot prompt
codex exec "Explain this repository structure."
```

### B) Gemini in Terminal

Prerequisite:
- Node.js 20+ installed.

Install Gemini CLI:

```bash
# Option 1: npm
npm install -g @google/gemini-cli

# Option 2: Homebrew (macOS/Linux)
brew install gemini-cli
```

Run Gemini:

```bash
gemini
```

On first run, choose **Login with Google** in the prompt flow.

Run without global install:

```bash
npx @google/gemini-cli
```

### C) Quick checks if command fails

```bash
codex --version
gemini --version
```

If `gemini` is not found after npm install, restart terminal and retry.

## 10) Official References

- Codex CLI repo: https://github.com/openai/codex
- Codex CLI getting started (OpenAI Help): https://help.openai.com/en/articles/11096431-openai-codex-ci-getting-started
- Gemini CLI repo: https://github.com/google-gemini/gemini-cli
- Gemini CLI get started: https://google-gemini.github.io/gemini-cli/docs/get-started/
