export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc
maturin build --release --target x86_64-unknown-linux-gnu -f #-i $HOME/.pyenv/shims/python3.10