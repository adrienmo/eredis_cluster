# eredis_cluster

## Description

eredis_cluster is a wrapper for eredis to support cluster mode of redis 3.0.0+
This project is under development

**Todo**

- Use connection pool (Probably poolboy)
- Add tests

## Compilation

The directory contains a Makefile and rebar

	make

## Usage

	%% Start the application
	eredis_cluster:start(),

	%% connect to one node of the cluster, it will retrieve all the node config
	%% using the command CLUSTER SLOTS
	eredis_cluster:connect([{"127.0.0.1",30001},{"127.0.0.1",30002}]),

	%% Use like eredis
	eredis_cluster:q(["GET","abc"]).
