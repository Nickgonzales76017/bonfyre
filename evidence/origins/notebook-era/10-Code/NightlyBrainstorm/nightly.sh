#!/bin/zsh
# Wrapper so launchd runs nightly.pl via /bin/zsh (which has FDA)
export PATH="/Users/nickgonzales/Library/Python/3.9/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
cd /Users/nickgonzales/Documents/Bonfyre/10-Code/NightlyBrainstorm
exec /usr/bin/perl nightly.pl "$@"
