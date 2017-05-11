lem-mariadb
============


About
-----

lem-mariadb is a MariaDB/MySQL library for the [Lua Event Machine][lem].
It allows you to query MariaDB and MySQL databases without blocking
other coroutines. It works with all versions of MariaDB and MySQL
servers, but it needs the MariaDB client library, as the MySQL client
library does not have a non-blocking interface.

[lem]: https://github.com/esmil/lem

Installation
------------

Get the source and do

    make
    make install

This installs the library under `/usr/local/lib/lua/5.1/`.
Use

    make PREFIX=<your custom path> install

to install to `<your custom path>/lib/lua/5.1/`.


Usage
-----

Import the module using something like

    local db = require 'lem.mariadb'

This sets `db` to a table with a single function.

* __db.connect{host=HOST, user=USER, passwd=PASSWORD, db=SCHEMA, port=PORT, socket=SOCKET_PATH)__

  Connect to the database using the given parameters. If any parameter
  is omitted, a default is used. Returns a new database connection
  object on success, or otherwise `nil` followed by an error message
  (string) and error code (integer).

The metatable of database connection objects can be found under
__db.Connection__. Prepared statements metatable (see below) is under
__db.PrepStmt__.

Database connection objects has the following methods.

* __db:close()__

  Close the database connection.

  Returns `true` on success or otherwise `nil, 'already closed'`,
  if the connection was already closed.

* __db:exec(query)__

  Execute an SQL query, and return the result as a Lua table with
  entries 1, 2, ... for each rows. Each row is again a Lua table with
  entries 1, 2, ... for each returned column. If there is no result
  set (eg. CREATE TABLE), the value `true` is returned instead of a Lua table.

  This method does not support placeholders (see db:prepare() /
  stmt:run() for that).

  In case of error, nil is returned followed by an error message and
  error code.

  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.

* __db:prepare(query)__

  prepares an SQL statement. Returns a new prepared statement object,
  or nil followed by an error message and error code in case of error.

  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.

Prepared statement objects have the following method:

* __stmt:run(...)__

  Executes the prepared statement with the given parameters. Each
  parameter is substituted for the corresponding placeholder '?' in
  the original SQL query that was prepared.

  The result is returned as a Lua table with entries 1, 2, ... for
  each rows. Each row is again a Lua table with entries 1, 2, ... for
  each returned column. If there is no result set (eg. CREATE TABLE),
  the value `true` is returned instead of a Lua table.

  In case of error, nil is returned followed by an error message and
  error code.

  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.


License
-------

lem-mariadb is free software. It is distributed under the terms of the
[GNU Lesser General Public License][lgpl].

[lgpl]: http://www.gnu.org/licenses/lgpl.html


Contact
-------

Kristian Nielsen <knielsen@knielsen-hq.org>
