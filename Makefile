
.ONESHELL:
PYTHON := ${PWD}/venv/bin/python3.8
PIP := ${PWD}/venv/bin/pip3.8

venv:
	@echo "Inicializa uma venv local."
	virtualenv env -p python3.8

activate: 
	$ ./env/Scripts/activate

install: 
	$ make venv
	$ make activate
	@echo "Instala as dependências numa venv local."
	${PIP} install -r requirements.txt

clean: 
	@echo "Remove a venv local."
	rm -rf env

.PHONY: activate
