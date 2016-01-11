# eredis_cluster

## Description

eredis_cluster is a wrapper for eredis to support cluster mode of redis 3.0.0+
This project is under development.
Travis CI build status: [![Build Status](https://travis-ci.org/adrienmo/eredis_cluster.svg?branch=master)](https://travis-ci.org/adrienmo/eredis_cluster)

## TODO

- Fix/Add specs of functions
- Add safeguard if keys of a pipeline command is not located in the same server
- Improve unit tests

## Compilation && Test

The directory contains a Makefile and rebar3

	make
	make test

## Configuration

To configure the redis cluster, you can use an application variable (probably in your app.config):

	{eredis_cluster,
	    [
	        {init_nodes,[
	            {"127.0.0.1",30001},
	            {"127.0.0.1",30002}
	        ]},
	        {pool_size, 5},
	        {pool_max_overflow, 0}
	    ]
	}

You don't need to specify all nodes of your configuration as eredis_cluster will
retrieve them through the command `CLUSTER SLOTS` at runtime.

## Usage

	%% Start the application
	eredis_cluster:start().

	%% Use like eredis
	eredis_cluster:q(["GET","abc"]).

	%% Pipeline
	eredis_cluster:qp([["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

	%% Transaction
	eredis_cluster:transaction([["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).
