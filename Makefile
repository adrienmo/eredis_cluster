ERL=erl
BEAMDIR=./deps/*/ebin ./ebin
REBAR=./rebar3

all: clean compile xref

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref skip_deps=true

clean: 
	@ $(REBAR) clean

eunit:
	@rm -rf .eunit
	@mkdir -p .eunit
	@$(REBAR) eunit 

test: eunit

edoc:
	@$(REBAR) skip_deps=true doc
