/* pgha--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgha" to load this file. \quit

CREATE SCHEMA pgha;

CREATE FUNCTION pgha.add_node(name text, conninfo text)
RETURNS bool
AS 'MODULE_PATHNAME', 'add_node'
LANGUAGE C STRICT;

CREATE FUNCTION pgha.del_node(name text)
RETURNS bool
AS 'MODULE_PATHNAME', 'del_node'
LANGUAGE C STRICT;

CREATE FUNCTION pgha.join_node(
IN name text,
IN conninfo text,
OUT name text,
OUT conninfo text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'join_node'
LANGUAGE C STRICT;

