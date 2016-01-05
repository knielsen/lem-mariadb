/*
 * This file is part of lem-postgres.
 * Copyright 2011 Emil Renner Berthing
 * Copyright 2013 Asbjørn Sloth Tønnesen
 *
 * lem-postgres is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * lem-postgres is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with lem-postgres. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdlib.h>
#include <assert.h>
#include <lem.h>
#include <mysql/mysql.h>

struct db {
	struct ev_io w;
	MYSQL conn_obj;
	MYSQL *conn;
};

static int
err_closed(lua_State *T)
{
	lua_pushnil(T);
	lua_pushliteral(T, "closed");
	return 2;
}

static int
err_busy(lua_State *T)
{
	lua_pushnil(T);
	lua_pushliteral(T, "busy");
	return 2;
}

static int
err_connection(lua_State *T, MYSQL *conn)
{
	const char *msg = mysql_error(conn);
	const char *p;

	for (p = msg; *p != '\n' && *p != '\0'; p++);

	lua_pushnil(T);
	if (p > msg)
		lua_pushlstring(T, msg, p - msg);
	else
		lua_pushliteral(T, "unknown error");
	return 2;
}

static int
db_gc(lua_State *T)
{
	struct db *d = lua_touserdata(T, 1);

	if (d->conn != NULL)
		mysql_close(d->conn);

	return 0;
}

static int
db_close(lua_State *T)
{
	struct db *d;
	lua_State *S;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);

	S = d->w.data;
	if (S != NULL) {
		ev_io_stop(LEM_ &d->w);
		lua_pushnil(S);
		lua_pushliteral(S, "interrupted");
		lem_queue(S, 2);
		d->w.data = NULL;
	}

	mysql_close(d->conn);
	d->conn = NULL;

	lua_pushboolean(T, 1);
	return 1;
}

static int
mysql_status(int events)
{
  int status= 0;
  if (events & EV_READ)
    status|= MYSQL_WAIT_READ;
  if (events & EV_WRITE)
    status|= MYSQL_WAIT_WRITE;
  if (events & EV_TIMEOUT)
    status|= MYSQL_WAIT_TIMEOUT;
  return status;
}

static void
db_handle_polling(struct db *d, int status)
{
	/* ToDo: Do we need separate watchers for read and write (and timeout) ? */
	if (status & MYSQL_WAIT_READ) {
		lem_debug("MARIA_POLLING_READING");
		ev_io_set(&d->w, mysql_get_socket(d->conn), EV_READ);
	}
	if (status & MYSQL_WAIT_WRITE) {
		lem_debug("PGRES_POLLING_WRITING");
		ev_io_set(&d->w, mysql_get_socket(d->conn), EV_WRITE);
	}
	/* ToDo: handle MYSQL_WAIT_TIMEOUT */
}

static void
mariadb_connect_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	int status;
	MYSQL *conn_res;

	ev_io_stop(EV_A_ &d->w);
	status = mysql_real_connect_cont(&conn_res, &d->conn_obj, mysql_status(revents));
	if (status) {
		db_handle_polling(d, status);
		ev_io_start(EV_A_ &d->w);
		return;
	}

	if (!conn_res) {
		lem_debug("MARIA_POLLING_FAILED");
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		d->conn = NULL;
		return;
	}

	lem_debug("MARIA_POLLING_OK");
	lem_queue(T, 1);
	d->w.data = NULL;
}

static int
mariadb_connect(lua_State *T)
{
	const char *o_host = luaL_checkstring(T, 1);
	const char *o_user = luaL_checkstring(T, 2);
	const char *o_passwd = luaL_checkstring(T, 3);
	const char *o_db = luaL_checkstring(T, 4);
	lua_Integer o_port = luaL_checkinteger(T, 5);
	const char *o_socket = luaL_checkstring(T, 6);
	MYSQL *conn, *conn_res;
	struct db *d;
	int status;

	lua_settop(T, 0);
	d = lua_newuserdata(T, sizeof(struct db));
	conn = d->conn = &d->conn_obj;
	lua_pushvalue(T, lua_upvalueindex(1));
	lua_setmetatable(T, -2);

	mysql_init(conn);
	mysql_options(conn, MYSQL_OPT_NONBLOCK, 0);
	status = mysql_real_connect_start(&conn_res, conn, o_host, o_user,
					  o_passwd, o_db, o_port, o_socket, 0);
	if (status) {
		ev_init(&d->w, mariadb_connect_cb);
		db_handle_polling(d, status);
		d->w.data = T;
		ev_io_start(EV_A_ &d->w);
		return lua_yield(T, 1);
	}

	if (!conn_res) {
		lem_debug("MARIA_POLLING_FAILED");
		d->conn = NULL;
		err_connection(T, conn);
		return 2;
	}

	lem_debug("MARIA_POLLING_OK");
	/* ToDo: Is this ev_io_init() needed? Maybe for later calls? */
	ev_io_init(&d->w, NULL, mysql_get_socket(d->conn), 0);
	return 1;
}

static void
push_tuples(lua_State *T, struct PGresult *res)
{
#ifdef ToDo
	int rows = PQntuples(res);
	int columns = PQnfields(res);
	int i;

	lua_createtable(T, rows, 0);
	for (i = 0; i < rows; i++) {
		int j;

		lua_createtable(T, columns, 0);
		for (j = 0; j < columns; j++) {
			if (PQgetisnull(res, i, j))
				lua_pushnil(T);
			else
				lua_pushlstring(T, PQgetvalue(res, i, j),
				                   PQgetlength(res, i, j));
			lua_rawseti(T, -2, j+1);
		}
		lua_rawseti(T, -2, i+1);
	}

	/* insert column names as "row 0" */
	lua_createtable(T, columns, 0);
	for (i = 0; i < columns; i++) {
		lua_pushstring(T, PQfname(res, i));
		lua_rawseti(T, -2, i+1);
	}
	lua_rawseti(T, -2, 0);
#endif
}

#ifdef ToDo
static void
db_error_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;

	(void)revents;

	if (PQconsumeInput(d->conn) == 0) {
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		return;
	}

	while (!PQisBusy(d->conn)) {
		PGresult *res = PQgetResult(d->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &d->w);
			lem_queue(T, 2);
			d->w.data = NULL;
			return;
		}

		PQclear(res);
	}
}
#endif

static void
db_exec_cb(EV_P_ struct ev_io *w, int revents)
{
#ifdef ToDo
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	PGresult *res;

	(void)revents;

	if (PQconsumeInput(d->conn) != 1) {
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		return;
	}

	while (!PQisBusy(d->conn)) {
		res = PQgetResult(d->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &d->w);
			lem_debug("returning %d values", lua_gettop(T) - 1);
			lem_queue(T, lua_gettop(T) - 1);
			d->w.data = NULL;
			return;
		}

		switch (PQresultStatus(res)) {
		case PGRES_EMPTY_QUERY:
			lem_debug("PGRES_EMPTY_QUERY");
			lua_settop(T, 0);
			lua_pushnil(T);
			lua_pushliteral(T, "empty query");
			goto error;

		case PGRES_COMMAND_OK:
			lem_debug("PGRES_COMMAND_OK");
			lua_pushboolean(T, 1);
			break;

		case PGRES_TUPLES_OK:
			lem_debug("PGRES_TUPLES_OK");
			push_tuples(T, res);
			break;

		case PGRES_COPY_IN:
			lem_debug("PGRES_COPY_IN");
			(void)PQsetnonblocking(d->conn, 1);
		case PGRES_COPY_OUT:
			lem_debug("PGRES_COPY_OUT");
			PQclear(res);
			lua_pushboolean(T, 1);
			lem_queue(T, lua_gettop(T) - 1);
			d->w.data = NULL;
			return;

		case PGRES_BAD_RESPONSE:
			lem_debug("PGRES_BAD_RESPONSE");
			lua_settop(T, 0);
			err_connection(T, d->conn);
			goto error;

		case PGRES_NONFATAL_ERROR:
			lem_debug("PGRES_NONFATAL_ERROR");
			break;

		case PGRES_FATAL_ERROR:
			lem_debug("PGRES_FATAL_ERROR");
			lua_settop(T, 0);
			err_connection(T, d->conn);
			goto error;

		default:
			lem_debug("unknown result status");
			lua_settop(T, 0);
			lua_pushnil(T);
			lua_pushliteral(T, "unknown result status");
			goto error;
		}

		PQclear(res);
	}

	lem_debug("busy");
	return;
error:
	PQclear(res);
	d->w.cb = db_error_cb;
	while (!PQisBusy(d->conn)) {
		res = PQgetResult(d->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &d->w);
			lem_queue(T, 2);
			d->w.data = NULL;
			return;
		}

		PQclear(res);
	}
#endif
}

void
prepare_params(lua_State *T, int n, const char **values, int *lengths)
{
	int i;

	for (i = 0; i < n; i++) {
		size_t len;
		const char *val;

		if (lua_isnil(T, i+3)) {
			val = NULL;
			/* len is ignored by libpq */
		} else {
			val = lua_tolstring(T, i+3, &len);
			if (val == NULL) {
				free(values);
				free(lengths);
				luaL_argerror(T, i+3, "expected nil or string");
				/* unreachable */
			}
		}

		values[i] = val;
		lengths[i] = len;
	}
}

static int
db_exec(lua_State *T)
{
#ifdef ToDo
	struct db *d;
	const char *command;
	int n;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	command = luaL_checkstring(T, 2);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	n = lua_gettop(T) - 2;
	if (n > 0) {
		const char **values = lem_xmalloc(n * sizeof(char *));
		int *lengths = lem_xmalloc(n * sizeof(int));

		prepare_params(T, n, values, lengths);

		n = PQsendQueryParams(d->conn, command, n,
		                      NULL, values, lengths, NULL, 0);
		free(values);
		free(lengths);
	} else
		n = PQsendQuery(d->conn, command);

	if (n != 1) {
		lem_debug("PQsendQuery failed");
		return err_connection(T, d->conn);
	}

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 1);
#endif
	return lua_yield(T, 1);
}

static int
db_prepare(lua_State *T)
{
#ifdef ToDo
	struct db *d;
	const char *name;
	const char *query;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	name = luaL_checkstring(T, 2);
	query = luaL_checkstring(T, 3);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);
	if (PQsendPrepare(d->conn, name, query, 0, NULL) != 1)
		return err_connection(T, d->conn);

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 1);
#endif
	return lua_yield(T, 1);
}

static int
db_run(lua_State *T)
{
#ifdef ToDo
	struct db *d;
	const char *name;
	int n;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	name = luaL_checkstring(T, 2);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	n = lua_gettop(T) - 2;
	if (n > 0) {
		const char **values = lem_xmalloc(n * sizeof(char *));
		int *lengths = lem_xmalloc(n * sizeof(int));

		prepare_params(T, n, values, lengths);

		n = PQsendQueryPrepared(d->conn, name, n,
				values, lengths, NULL, 0);
		free(values);
		free(lengths);
	} else
		n = PQsendQueryPrepared(d->conn, name, 0, NULL, NULL, NULL, 0);

	if (n != 1)
		return err_connection(T, d->conn);

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 1);
#endif
	return lua_yield(T, 1);
}

int
luaopen_lem_mariadb(lua_State *L)
{
	lua_newtable(L);

	/* create Connection metatable mt */
	lua_newtable(L);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* mt.__gc = <db_gc> */
	lua_pushcfunction(L, db_gc);
	lua_setfield(L, -2, "__gc");
	/* mt.close = <db_close> */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "close");
	/* mt.exec = <db_exec> */
	lua_pushcfunction(L, db_exec);
	lua_setfield(L, -2, "exec");
	/* mt.prepare = <db_prepare> */
	lua_pushcfunction(L, db_prepare);
	lua_setfield(L, -2, "prepare");
	/* mt.run = <db_run> */
	lua_pushcfunction(L, db_run);
	lua_setfield(L, -2, "run");

	/* connect = <mariadb_connect> */
	lua_pushvalue(L, -1); /* upvalue 1: Connection */
	lua_pushcclosure(L, mariadb_connect, 1);
	lua_setfield(L, -3, "connect");

	/* set Connection */
	lua_setfield(L, -2, "Connection");

	return 1;
}
