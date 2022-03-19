CONFIG_DIR := config
DB_DIR := db
DB := $(DB_DIR)/db.sqlite
DB_RAW := $(DB_DIR)/raw.sqlite

.PHONY: clean all install-virtualenv install-py-deps

all: $(DB) $(VENV)

$(DB_DIR):
	mkdir $(DB_DIR)

$(DB_RAW):
	sqlite3 "$(DB_RAW)" < $(CONFIG_DIR)/arxiv_raw_ddl.sql

$(DB): $(DB_DIR) $(DB_RAW)
	sqlite3 "$(DB)" "VACUUM"
	sqlite3 "$(DB)" "ATTACH DATABASE '$(DB_RAW)' AS raw"

install-virtualenv:
	pip install virtualenv

venv: install-virtualenv requirements.txt
	virtualenv --python=python3.9 venv
	. ./venv/bin/activate && pip install -r requirements.txt

venv_dbt: install-virtualenv requirements_dbt.txt
	virtualenv --python=python3.9 venv_dbt
	. ./venv_dbt/bin/activate && pip install -r requirements_dbt.txt
clean:
	@$(RM) -r $(DB_DIR) venv venv_dbt
