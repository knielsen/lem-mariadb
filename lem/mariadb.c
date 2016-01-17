/*
 * This file is part of lem-mariadb.
 * Copyright 2015 Kristian Nielsen
 * Copyright 2015 Emil Renner Berthing
 *
 * lem-mariadb is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * lem-mariadb is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with lem-mariadb. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <lem.h>
#include <mysql/mysql.h>

struct db {
	struct ev_io w;
	MYSQL conn_obj;
	MYSQL *conn;
	int step;
};

struct bind_data {
	unsigned long length;
	my_bool is_null;
	my_bool error;
	char buffer[4 /* ToDo: 4096 */];
};

struct stmt {
	struct db *d;
	MYSQL_STMT *my_stmt;
	MYSQL_BIND *param_bind, *result_bind;
	MYSQL_RES *result_metadata;
	struct bind_data *bind_data;
	int param_count;
	int num_fields;
	int row_idx;
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

	lua_pushnil(T);
        lua_pushlstring(T, msg, strlen(msg));
        lua_pushinteger(T, mysql_errno(conn));
	return 3;
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
	int flags = 0;
	if (status & MYSQL_WAIT_READ) {
		lem_debug("MARIA_POLLING_READING");
		flags |= EV_READ;
	}
	if (status & MYSQL_WAIT_WRITE) {
		lem_debug("PGRES_POLLING_WRITING");
		flags |= EV_WRITE;
	}
	/* ToDo: handle MYSQL_WAIT_TIMEOUT */
	ev_io_set(&d->w, mysql_get_socket(d->conn), flags);
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
	d->step = -1;
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
		lua_settop(T, 0);
		return err_connection(T, conn);
	}

	lem_debug("MARIA_POLLING_OK");
	/* ToDo: Is this ev_io_init() needed? Maybe for later calls? */
	ev_io_init(&d->w, NULL, mysql_get_socket(d->conn), 0);
	d->w.data = NULL;
	return 1;
}

static int
push_tuples(lua_State *T, MYSQL_RES *res)
{
	unsigned fields, i;
	int idx;

	lua_settop(T, 0);
	if (!res) {
		/* Empty result set (like UPDATE/DELETE/INSERT, or DDL). */
		lua_createtable(T, 0, 0);
		return 1;
	}

	lua_createtable(T, mysql_num_rows(res), 0);
	fields = mysql_num_fields(res);
	idx = 1;
	for (;;) {
		MYSQL_ROW row;
		unsigned long *lengths;

		row = mysql_fetch_row(res);
		if (!row)
			break;
		lengths = mysql_fetch_lengths(res);
		lua_createtable(T, fields, 0);
		for (i = 0; i < fields; ++i) {
			if (row[i])
				lua_pushlstring(T, row[i], lengths[i]);
			else
				lua_pushnil(T);
			lua_rawseti(T, -2, i+1);
		}
		lua_rawseti(T, -2, idx);
		++idx;
	}
	mysql_free_result(res);
	return 1;
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

#ifdef ToDo
static void
db_exec_cb(EV_P_ struct ev_io *w, int revents)
{
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
}
#endif

int
prepare_params(lua_State *T, struct stmt *st, int n)
{
	int i;
	int param_count;
	MYSQL_STMT *my_stmt = st->my_stmt;
	MYSQL_BIND *bind = st->param_bind;

	param_count = st->param_count;
	memset(bind, 0, param_count * sizeof(*bind));

	for (i = 0; i < param_count; i++) {
		size_t len;

		/*
		  On the stack, we have the stmt handle followed by the
		  parameters. So the first parameter is at index 2.
		*/
		if (i >= n || lua_isnil(T, i+2)) {
			bind[i].buffer_type = MYSQL_TYPE_NULL;
		} else {
			bind[i].buffer_type = MYSQL_TYPE_STRING;
			bind[i].is_null = NULL;
			bind[i].buffer = (char *)lua_tolstring(T, i+2, &len);
			bind[i].buffer_length = len;
			bind[i].length = &bind[i].buffer_length;
		}
	}
	if (mysql_stmt_bind_param(my_stmt, bind))
		return 1;
	return 0;
}

#ifdef ToDo
static int
db_exec(lua_State *T)
{
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
	return lua_yield(T, 1);
}
#endif

static int
wrap_stmt(lua_State *T, struct stmt *st)
{
	MYSQL_RES *res;
	MYSQL_STMT *my_stmt = st->my_stmt;

	res = mysql_stmt_result_metadata(my_stmt);
	if (!res) {
		lua_settop(T, 1);
		return err_connection(T, st->d->conn);
	}
	st->result_metadata = res;
	st->param_count = mysql_stmt_param_count(my_stmt);
	st->param_bind = lem_xmalloc(st->param_count * sizeof(MYSQL_BIND));
	st->num_fields = mysql_num_fields(res);
	st->result_bind = lem_xmalloc(st->num_fields * sizeof(MYSQL_BIND));
	st->bind_data = lem_xmalloc(st->num_fields * sizeof(struct bind_data));
	st->row_idx = -1;
	return 1;
}

static void
db_prepare_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	struct stmt *st;
	lua_State *T = d->w.data;
	int status;
	int err = 0;
	MYSQL *conn = d->conn;
	MYSQL_STMT *my_stmt;

	st = lua_touserdata(T, 3);
	my_stmt = st->my_stmt;

	ev_io_stop(EV_A_ &d->w);
	status = mysql_stmt_prepare_cont(&err, my_stmt, mysql_status(revents));
	if (status) {
		db_handle_polling(d, status);
		ev_io_start(EV_A_ &d->w);
		return;
	}

	d->w.data = NULL;
	if (err) {
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, conn));
		return;
	}

	lem_queue(T, wrap_stmt(T, st));
}

static int
db_prepare(lua_State *T)
{
	struct db *d;
	const char *query;
	int err;
	MYSQL *conn;
	int status;
	MYSQL_STMT *my_stmt;
	struct stmt *st;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	query = luaL_checkstring(T, 2);
	d = lua_touserdata(T, 1);
	conn = d->conn;
	if (conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	my_stmt = mysql_stmt_init(conn);
	if (!my_stmt)
		return err_connection(T, conn);

	/* Put the prepared statement object on top of the stack */
	st = lua_newuserdata(T, sizeof(struct stmt));
	st->d = d;
	st->my_stmt = my_stmt;
	st->param_bind = st->result_bind = NULL;
	st->bind_data = NULL;
	st->param_count = -1;
	st->num_fields = -1;
	st->result_metadata = NULL;
	/* ToDo: refcounting to avoid dangling st->conn reference */
	lua_pushvalue(T, lua_upvalueindex(1));
	lua_setmetatable(T, -2);

	status = mysql_stmt_prepare_start(&err, my_stmt, query, strlen(query));
	if (!status)
		return wrap_stmt(T, st);
	d->w.data = T;
	ev_set_cb(&d->w, db_prepare_cb);
	db_handle_polling(d, status);
	ev_io_start(LEM_ &d->w);
	/* Yield with 3 items on the stack: db, query, stmt. */
	return lua_yield(T, 3);
}

static void
db_exec_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	int status;
	int err = 0;
	MYSQL_RES *res = NULL;
	MYSQL *conn = d->conn;
        int step = d->step;

	ev_io_stop(EV_A_ &d->w);
	if (step == 0)
		status = mysql_real_query_cont(&err, conn, mysql_status(revents));
	else
		status = mysql_store_result_cont(&res, conn, mysql_status(revents));
	if (step == 0 && !status && !err) {
		step = d->step = 1;
		status = mysql_store_result_start(&res, conn);
	}
	if (status) {
		db_handle_polling(d, status);
		ev_io_start(EV_A_ &d->w);
		return;
	}

	d->w.data = NULL;
	d->step = -1;
	if ((step == 0 && err) || (step == 1 && !res && mysql_errno(conn) != 0)) {
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, conn));
		return;
	}

	lem_queue(T, push_tuples(T, res));
}

static int
db_exec(lua_State *T)
{
	struct db *d;
	const char *query;
	int err;
	MYSQL *conn;
	MYSQL_RES *res;
	int status;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	query = luaL_checkstring(T, 2);

	d = lua_touserdata(T, 1);
	conn = d->conn;
	if (conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	status = mysql_real_query_start(&err, conn, query, strlen(query));
	if (!status) {
		if (err) {
			lua_settop(T, 0);
			return err_connection(T, conn);
		}
		status = mysql_store_result_start(&res, conn);
		if (!status) {
			if (!res && mysql_errno(conn) != 0) {
				lua_settop(T, 0);
				return err_connection(T, conn);
			}
			return push_tuples(T, res);
		} else
			d->step = 1;
	} else
		d->step = 0;

	d->w.data = T;
	ev_set_cb(&d->w, db_exec_cb);
	db_handle_polling(d, status);
	ev_io_start(LEM_ &d->w);
	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static int
stmt_gc(lua_State *T)
{
	/* ToDo */
	return 0;
}

static int
stmt_close(lua_State *T)
{
	struct stmt *st;

	/* ToDo */
	if (st->result_metadata)
		mysql_free_result(st->result_metadata);
	if (st->my_stmt)
		mysql_stmt_close(st->my_stmt);
	free(st->param_bind);
	free(st->result_bind);
	free(st->bind_data);
	return 0;
}

static int
bind_params(lua_State *T, struct db *d, struct stmt *st)
{
	MYSQL_RES *res;
	unsigned num_fields;
	unsigned i;
	struct bind_data *bind_data;
	MYSQL_BIND *binds;

	res = st->result_metadata;
	num_fields = st->num_fields;
	if (num_fields == 0)
		return 1;			/* No result set */

	binds = st->result_bind;
	bind_data = st->bind_data;
	memset(binds, 0, num_fields * sizeof(MYSQL_BIND));
	memset(bind_data, 0, num_fields * sizeof(struct bind_data));
	for (i = 0; i < num_fields; ++i) {
		MYSQL_FIELD *field = mysql_fetch_field_direct(res, i);
		MYSQL_BIND *b = &binds[i];

		if (!field)
			return err_connection(T, d->conn);
		/*
		  ToDo: We could maybe look at the field type, and fetch an
		  integer or number of numeric types. Though it can be a bit
		  tricky to ensure that the number will not over/underflow
		  (32/64 bit platform, signed/unsigned, DECIMAL, ...).
		*/
		b->buffer_type = MYSQL_TYPE_STRING;
		b->buffer = bind_data[i].buffer;
		b->buffer_length = sizeof(bind_data[i].buffer);
		b->is_null = &bind_data[i].is_null;
		b->length = &bind_data[i].length;
		b->error = &bind_data[i].error;
	}
	if (mysql_stmt_bind_result(st->my_stmt, binds))
		return err_connection(T, d->conn);
	return -1;
}

static int
push_stmt_tuple(lua_State *T, struct db *d, struct stmt *st, int err)
{
	unsigned num_fields;
	unsigned i;
	MYSQL_STMT *my_stmt = st->my_stmt;

	/* Top of the stack is the result table. We need to add one row to it. */
	num_fields = mysql_num_fields(st->result_metadata);
	lua_createtable(T, num_fields, 0);
	for (i = 0; i < num_fields; ++i) {
		struct bind_data *bd = &st->bind_data[i];
		char *p = 0;

		/*
		  If data did not fit in our buffer, we need to allocate a
		  larger buffer for it.
		*/
		if (err == MYSQL_DATA_TRUNCATED && bd->error) {
			MYSQL_BIND extra_bind;
			memset(&extra_bind, 0, sizeof(extra_bind));
			extra_bind.buffer_type = MYSQL_TYPE_STRING;
			extra_bind.buffer = p = lem_xmalloc(bd->length);
			extra_bind.buffer_length = bd->length;
			extra_bind.is_null = &bd->is_null;
			extra_bind.length = &bd->length;
			extra_bind.error = &bd->error;
			if (mysql_stmt_fetch_column(my_stmt, &extra_bind, i, 0))
				return err_connection(T, d->conn);
		}
		if (bd->is_null)
			lua_pushnil(T);
		else
			lua_pushlstring(T, (p ? p : bd->buffer), bd->length);
		if (p)
			free(p);
		lua_rawseti(T, -2, i+1);
	}
	lua_rawseti(T, -2, ++st->row_idx);
	return -1;
}

/*
  Perform the next step in stmt:exec().
  This can be:
   - mysql_stmt_exec_cont(), if we are still executing the statement
   - mysql_stmt_fetch_start(), to start fetching the next row
   - mysql_stmt_fetch_cont(), to continue row fetching
   - Ending, when all rows have been fetch.

  Returns -1 if there is still more to be done; else the number of return
  values.
*/
static int
stmt_run_next_step(int status, int err, lua_State *T, struct db *d, struct stmt *st)
{
	MYSQL_STMT *my_stmt = st->my_stmt;
	int step = d->step;
	int num_return_values;

	for (;;) {
		if (status) {
			db_handle_polling(d, status);
			ev_io_start(LEM_ &d->w);
			return -1;
		}
		if (step == 0) {
			if (err) {
				lua_settop(T, 0);
				return err_connection(T, d->conn);
			} else {
				/* Allocate the result table */
				lua_newtable(T);
				num_return_values = bind_params(T, d, st);
				if (num_return_values >= 0)
					return num_return_values;
				status = mysql_stmt_fetch_start(&err, my_stmt);
				step = d->step = 1;
				continue;
			}
		}
		/* step == 1 */
		if (!err || err == MYSQL_DATA_TRUNCATED) {
			num_return_values = push_stmt_tuple(T, d, st, err);
			if (num_return_values >= 0)
				return num_return_values;
			status = mysql_stmt_fetch_start(&err, my_stmt);
			continue;
		}
		if (err == MYSQL_NO_DATA) {
			return 1;
		}
		/* err == 1, error occured. */
		lua_settop(T, 0);
		return err_connection(T, d->conn);
	}
}

static void
stmt_run_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	int status;
	int err = 0;
	struct stmt *st;
	MYSQL_STMT *my_stmt;
        int step = d->step;
	int num_results;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	st = lua_touserdata(T, 1);
	my_stmt = st->my_stmt;

	ev_io_stop(EV_A_ &d->w);
	if (step == 0)
		status = mysql_stmt_execute_cont(&err, my_stmt, mysql_status(revents));
	else
		status = mysql_stmt_fetch_cont(&err, my_stmt, mysql_status(revents));
	num_results = stmt_run_next_step(status, err, T, d, st);
	if (num_results < 0)
		return;
	d->w.data = NULL;
	d->step = -1;
	lem_queue(T, num_results);
}

static int
stmt_run(lua_State *T)
{
	struct stmt *st;
	struct db *d;
	int err;
	MYSQL *conn;
	MYSQL_STMT *my_stmt;
	int status;
	int n;
	int num_results;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	st = lua_touserdata(T, 1);
	d = st->d;
	conn = d->conn;
	my_stmt = st->my_stmt;
	if (conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	n = lua_gettop(T) - 1;
	if (prepare_params(T, st, n)) {
		lua_settop(T, 0);
		return err_connection(T, conn);
	}
	status = mysql_stmt_execute_start(&err, my_stmt);
	st->row_idx = 0;
	d->step = 0;
	d->w.data = T;
	ev_set_cb(&d->w, stmt_run_cb);
	num_results = stmt_run_next_step(status, err, T, d, st);
	if (num_results < 0)
		return lua_yield(T, lua_gettop(T));
	d->step = -1;
	d->w.data = NULL;
	return num_results;
}

int
luaopen_lem_mariadb(lua_State *L)
{
	lua_createtable(L, 0, 2);

	/* Create prepared statement metatable stmt */
	lua_createtable(L, 0, 4);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* stmt.__gc = <stmt_gc> */
	lua_pushcfunction(L, stmt_gc);
	lua_setfield(L, -2, "__gc");
	/* stmt.close = <stmt_close> */
	lua_pushcfunction(L, stmt_close);
	lua_setfield(L, -2, "close");
	/* stmt.run = <stmt_run> */
	lua_pushcfunction(L, stmt_run);
	lua_setfield(L, -2, "run");

	/* set PrepStmt */
	lua_setfield(L, -2, "PrepStmt");

	/* create Connection metatable mt */
	lua_createtable(L, 0, 5);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* mt.__gc = <db_gc> */
	lua_pushcfunction(L, db_gc);
	lua_setfield(L, -2, "__gc");
	/* mt.close = <db_close> */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "close");
	/* mt.prepare = <db_prepare> */
	lua_getfield(L, -2, "PrepStmt");  /* upvalue 1: PrepStmt */
	lua_pushcclosure(L, db_prepare, 1);
	lua_setfield(L, -2, "prepare");
	/* mt.exec = <db_exec> */
	lua_pushcfunction(L, db_exec);
	lua_setfield(L, -2, "exec");

	/* connect = <mariadb_connect> */
	lua_pushvalue(L, -1); /* upvalue 1: Connection */
	lua_pushcclosure(L, mariadb_connect, 1);
	lua_setfield(L, -3, "connect");

	/* set Connection */
	lua_setfield(L, -2, "Connection");

	return 1;
}
