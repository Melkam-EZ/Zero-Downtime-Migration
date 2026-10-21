.PHONY: setup up demo verify test down
setup:  ; scripts/setup.sh
up:     ; scripts/up.sh
demo:   ; scripts/demo.sh
verify: ; scripts/verify.sh
test:   ; .venv/bin/python -m pytest -q
down:   ; scripts/down.sh
