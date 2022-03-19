# TODO: create ~/.dbt/profiles.yml via jinja

.PHONY: clean all install-virtualenv

all: venv venv_dbt

install-virtualenv:
	pip install virtualenv

venv: install-virtualenv requirements.txt
	virtualenv --python=python3.9 venv
	. ./venv/bin/activate && pip install -r requirements.txt

venv_dbt: install-virtualenv requirements_dbt.txt
	virtualenv --python=python3.9 venv_dbt
	. ./venv_dbt/bin/activate && pip install -r requirements_dbt.txt

clean:
	@$(RM) -r venv venv_dbt
