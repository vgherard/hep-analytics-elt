DB_DIR := db
SQLITE_DB := $(DB_DIR)/hepanalytics.sqlite
VENV := venv

.PHONY: clean all install-virtualenv install-py-deps

all: $(SQLITE_DB) $(VENV)

$(DB_DIR):
	mkdir $(DB_DIR)
$(SQLITE_DB): $(DB_DIR)
	sqlite3 "$(SQLITE_DB)" "VACUUM"

install-virtualenv:
	pip install virtualenv

$(VENV): install-virtualenv requirements.txt
	virtualenv --python=python3.9 $(VENV)
	. ./venv/bin/activate && pip install -r requirements.txt

clean:
	@$(RM) -r $(DB_DIR) $(VENV)
