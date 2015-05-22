ERL=erl
BEAMDIR=./deps/*/ebin ./ebin
REBAR=./rebar

all: clean get-deps update-deps compile xref

update-deps:
	@$(REBAR) update-deps

get-deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref skip_deps=true

clean: 
	@ $(REBAR) clean

eunit:
	@rm -rf .eunit
	@mkdir -p .eunit
	@ERL_FLAGS="-config test.config" $(REBAR) skip_deps=true eunit 

test: eunit

edoc:
	@$(REBAR) skip_deps=true doc
