--
-- This file is part of lem-mariadb.
-- Copyright 2011-2016 Emil Renner Berthing
-- Copyright 2016 Kristian Nielsen
--
-- lem-mariadb is free software: you can redistribute it and/or
-- modify it under the terms of the GNU General Public License as
-- published by the Free Software Foundation, either version 3 of
-- the License, or (at your option) any later version.
--
-- lem-mariadb is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with lem-mariadb.  If not, see <http://www.gnu.org/licenses/>.
--

local utils   = require 'lem.utils'
local mariadb  = require 'lem.mariadb'

local setmetatable = setmetatable
local thisthread, suspend, resume
	= utils.thisthread, utils.suspend, utils.resume

local Connection = {}
Connection.__index = Connection

function Connection:close(...)
	return self.db:close(...)
end

local PrepStmt = {}
PrepStmt.__index = PrepStmt

do
	local function get(queue)
		local nxt = queue.tail
		if nxt == 0 then
			nxt = 1
			queue.tail = 1
		else
			local me = nxt

			queue[me] = thisthread()
			nxt = #queue+1
			queue.tail = nxt
			suspend()
			queue[me] = nil
		end

		queue.next = nxt
		return queue.db
	end
	Connection.get = get

	local function put(queue, ...)
		local T = queue[queue.next]
		if T then
			resume(T)
		else
			queue.tail = 0
		end
		return ...
	end
	Connection.put = put

	function Connection:prepare(...)
		local stmt, msg, err = put(self, get(self):prepare(...))
		if not stmt then return nil, msg, err end
		return setmetatable({ stmt = stmt, queue = self }, PrepStmt)
	end

	local function db_wrap(method)
		return function(queue, ...)
			return put(queue, method(get(queue), ...))
		end
	end

	Connection.exec = db_wrap(mariadb.Connection.exec)

	function PrepStmt:get()
		get(self.queue)
		return self.stmt
	end

	function PrepStmt:put(...)
		return put(self.queue, ...)
	end

	local function stmt_wrap(method)
		return function(self, ...)
			local queue = self.queue
			get(queue)
			return put(queue, method(self.stmt, ...))
		end
	end

	PrepStmt.run = stmt_wrap(mariadb.PrepStmt.run)
end

local connect
do
	local uconnect = mariadb.connect

	function connect(...)
		local db, msg, err = uconnect(...)
		if not db then return nil, msg, err end

		return setmetatable({ db = db, tail = 0, next = 0 }, Connection)
	end
end

return {
	Connection   = Connection,
	PrepStmt     = PrepStmt,
	connect      = connect,
}

-- vim: set ts=2 sw=2 noet:
