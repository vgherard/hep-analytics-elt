DB_DIR := db
SQLITE_DB := $(DB_DIR)/arxivhepabs.sqlite
VENV_DIR := venv

.PHONY: clean all install-virtualenv install-py-deps

all: $(SQLITE_DB) install-py-deps

$(DB_DIR):
	mkdir $(DB_DIR)
$(SQLITE_DB): $(DB_DIR)
	sqlite3 "$(SQLITE_DB)" "VACUUM"

install-virtualenv:
	pip install virtualenv

$(VENV_DIR): install-virtualenv
	virtualenv --python=python3.9 ./venv

install-py-deps: $(VENV_DIR)
	. ./venv/bin/activate && pip install -r requirements.txt

clean:
	@$(RM) -r $(DB_DIR)
