# Shell Aliases for Claude Code

## PowerShell (recommended)

Add these functions to `$PROFILE`:

```powershell
function cs { claude "/prime" }
function cr { claude --dangerously-skip-permissions "/prime" }
```

Reload profile:

```powershell
. $PROFILE
```

## Bash/Zsh (optional)

```bash
alias cs='claude "/prime"'
alias cr='claude --dangerously-skip-permissions "/prime"'
```
