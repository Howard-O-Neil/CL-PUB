# ~/.bashrc: executed by bash(1) for non-login shells.

# Note: PS1 and umask are already set in /etc/profile. You should not
# need this unless you want different defaults for root.
# PS1='${debian_chroot:+($debian_chroot)}\h:\w\$ '
# umask 022

PS1='\[\033[01;32m\]\u\[\033[00m\]:\[\033[01;31m\]\h\[\033[01;34m\] \W \$\[\033[00m\] '

# You may uncomment the following lines if you want `ls' to be colorized:
export LS_OPTIONS='--color=auto'
# eval "$(dircolors)"
alias ls='ls $LS_OPTIONS'
alias ll='ls $LS_OPTIONS -l'
alias l='ls $LS_OPTIONS -lA'

export PATH="${PATH}:/usr/local/cuda/bin"
export PATH="${PATH}:/usr/local/TensorRT-8.2.0.6/bin"