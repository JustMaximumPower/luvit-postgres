--------------------------------------------------------------------------
-- This module is a luvit binding for the postgresql api. 
-- 
-- Copyright (C) 2012 Moritz Kühner, Germany.
-- Permission is hereby granted, free of charge, to any person obtaining
-- a copy of this software and associated documentation files (the
-- "Software"), to deal in the Software without restriction, including
-- without limitation the rights to use, copy, modify, merge, publish,
-- distribute, sublicense, and/or sell copies of the Software, and to
-- permit persons to whom the Software is furnished to do so, subject to
-- the following conditions:
--
-- The above copyright notice and this permission notice shall be
-- included in all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
-- EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
-- MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
-- IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
-- CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
-- TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
-- SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
--------------------------------------------------------------------------

local postgres = require("postgresffi")
local timer = require('timer')
local Emitter = require('core').Emitter
local table = require("table")

postgres.init()

--Timeout for the poll timer
local POLLTIMER_INTERVALL = 20

--Pool of unused connections
local conPool = {}

local LuvPostgres = Emitter:extend()

function LuvPostgres:initialize(coninfo)
    if #conPool > 0 then
        self.con = conPool[#conPool]
        conPool[#conPool] = nil
    else
        self.con = postgres.newAsync(coninfo)
    end
    
    --[[ This is an dirty hack to update the connection state. The correct
         solution should watch the socket descriptor and update upon 
         network activity    
    ]]
    self.watcher = timer.setInterval(POLLTIMER_INTERVALL, function()
        local state = self.con:dialUpState()
        if 0 == state then
            timer.clearTimer(self.watcher)
            self.established = true
            self:emit("established")
        elseif 1 == state then
            timer.clearTimer(self.watcher)
            self:emit("error", self.con:getError())
        end
    end)
end

function LuvPostgres:sendQuery(query)
    if not self.established then
        slef:emit("error", "Can't send query. Connection is not established!")
        return
    end
    self.con:sendQuery(query)
    
    --[[ This is an dirty hack to update the connection state. The correct
         solution should watch the socket descriptor and update upon 
         network activity    
    ]]
    self.watcher = timer.setInterval(POLLTIMER_INTERVALL, function()
        if self.con then
            local ok, ready = pcall(self.con.readReady, self.con)
            if not ok then
                self:emit("error", "")
            elseif ready then
                local ok, result , status = pcall(self.con.getAvailable, self.con)
                if not ok then
                    self:emit("error", result)
                else
                    if status <= 7 then
                        timer.clearTimer(self.watcher)
                        if self.con:getAvailable() ~= nil then
                            self:emit("error", "Internal binding error. Query is not over!")
                            return
                        end
                        
                        if status == 5 or status == 7 then 
                            self:emit("error", self.con:getError())
                        else
                            self:emit("result", result)
                            self:emit("finished")
                        end
                    else
                        self:emit("result", result)
                    end
                end
            end
         else
            timer.clearTimer(self.watcher)
         end
    end)
end


function LuvPostgres:escape(query)
    local ok, value = pcall(self.con.escape, self.con, query)
    if ok then
        return value
    end
    self:emit("error", value)
end


function LuvPostgres:release()
    if self.con.queryInProcess then
        p("connection is in a Bad state")
        p(self.con)
    else
        if self.established then
            table.insert(conPool, self.con)
        end
        self.con = nil
        self.watcher = nil
    end
end


return LuvPostgres
