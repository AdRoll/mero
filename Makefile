REBAR=rebar

all: deps compile xref

docs:
	erl -noshell -run edoc_run application "'$(APP_NAME)'" '"."' '$(VSN)' -s init stop

deps:
	@$(REBAR) update-deps
	@$(REBAR) get-deps

compile:
	$(REBAR) compile

deep_clean: logs_clean
	@rm -rf deps*
	@$(REBAR) clean

logs_clean:
	@rm -rf logs/*

clean:
	@$(REBAR) clean

ct:
	@$(REBAR)  skip_deps=true ct

eunit:
	@$(REBAR)  skip_deps=true eunit

test: logs_clean all ct eunit

testfast:
	@$(REBAR)  skip_deps=true ct
	@$(REBAR)  skip_deps=true eunit

xref:
	@$(REBAR) skip_deps=true xref
