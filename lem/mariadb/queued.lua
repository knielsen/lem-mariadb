--
-- This file is part of lem-mariadb.
-- Copyright 2011 Emil Renner Berthing
-- Copyright 2016 Kristian Nielsen

local utils    = require 'lem.utils'
local mariadb  = require 'lem.mariadb'

local remove = table.remove
local thisthread, suspend, resume = utils.thisthread, utils.suspend, utils.resume

local QConnection = {}
QConnection.__index = QConnection

function QConnection:close()
	local ok, err = self.conn:close()
	for i = 1, self.n - 1 do
		resume(self[i])
		self[i] = nil
	end
	return ok, err
end
QConnection.__gc = QConnection.close

local function lock(self)
	local n = self.n
	if n == 0 then
		self.n = 1
	else
		self[n] = thisthread()
		self.n = n + 1
		suspend()
	end
	return self.conn
end
QConnection.lock = lock

local function unlock(self, ...)
	local n = self.n - 1
	self.n = n
	if n > 0 then
		resume(remove(self, 1))
	end
	return ...
end
QConnection.unlock = unlock

local function wrap(method)
	return function(self, ...)
		return unlock(self, method(lock(self), ...))
	end
end

local Connection = mariadb.Connection
QConnection.exec    = wrap(Connection.exec)
QConnection.prepare = wrap(Connection.prepare)
QConnection.run     = wrap(Connection.run)

local qconnect
do
	local setmetatable = setmetatable
	local connect = mariadb.connect

	function qconnect(...)
		local conn, err = connect(...)
		if not conn then return nil, err end

		return setmetatable({
			n = 0,
			conn = conn,
		}, QConnection)
	end
end

return {
	QConnection = QConnection,
	connect = qconnect,
}

-- vim: ts=2 sw=2 noet:
