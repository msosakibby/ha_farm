#!/bin/bash

# Configuration
CONFIG_DIR="/config"
SSH_KEY="/config/.ssh/id_ed25519"
BRANCH="main"
LOG_FILE="/config/git_sync.log"

# Redirect all output to the log file for forensics
exec > "$LOG_FILE" 2>&1

echo "--- Starting Git Sync: $(date) ---"

# Force SSH to use the specific deploy key and bypass strict host checking
export GIT_SSH_COMMAND="ssh -i $SSH_KEY -o StrictHostKeyChecking=no"

cd $CONFIG_DIR || exit

# Force Git Identity to prevent interactive prompts
git config user.name "HA Auto Sync"
git config user.email "ha@local.domain"

git add .

if git diff-index --quiet HEAD --; then
    echo "No changes to sync."
else
    git commit -m "Automated backup: $(date)"
    
    echo "Pulling remote changes..."
    # The --autostash flag prevents rebase failures if active files (like the log) change during execution
    git pull origin $BRANCH --rebase --autostash
    
    echo "Pushing to GitHub..."
    git push origin $BRANCH
    
    echo "Sync Complete: $(date)"
fi
