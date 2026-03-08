#!/bin/bash
# Git hooks setup for dhtrader submodule
# The submodule's actual git directory is at ../.git/modules/dhtrader

HOOK_NAMES="applypatch-msg pre-applypatch post-applypatch pre-commit prepare-commit-msg commit-msg post-commit pre-rebase post-checkout post-merge pre-receive update post-receive post-update pre-auto-gc"

# Get the real git directory for this submodule
GIT_DIR=$(git rev-parse --git-dir)
HOOK_DIR="$GIT_DIR/hooks"

for hook in $HOOK_NAMES; do
    # If the hook already exists, is executable, and is not a symlink
    if [ ! -h $HOOK_DIR/$hook -a -x $HOOK_DIR/$hook ]; then
        mv $HOOK_DIR/$hook $HOOK_DIR/$hook.local
    fi
    # create the symlink, overwriting the file if it exists
    # For submodules, we need to use an absolute path since the git dir
    # is not in the working tree
    REPO_ROOT=$(git rev-parse --show-toplevel)
    ln -s -f "$REPO_ROOT/githooks/$hook" "$HOOK_DIR/$hook"
    echo $hook
done
