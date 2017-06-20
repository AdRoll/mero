REBAR=./rebar3

all: compile

docs:
	erl -noshell -run edoc_run application "'$(APP_NAME)'" '"."' '$(VSN)' -s init stop

compile:
	$(REBAR) compile

deep_clean:
	@rm -rf _build/*
	@$(REBAR) clean

clean:
	@$(REBAR) clean

.PHONY: test
test:
	@$(REBAR)  eunit
	@$(REBAR)  ct

xref:
	@$(REBAR) xref
