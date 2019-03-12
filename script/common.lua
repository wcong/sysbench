
function init()
   assert(event ~= nil,
          "this script is meant to be included by other wordpress scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, run, " ..
            "cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
   config_json =
      {"config_json", "config_json"},
   skip_trx =
      {"Don't start explicit transactions and execute all queries " ..
          "in the AUTOCOMMIT mode", false}
}

-- Prepare the dataset. This command don't supports parallel execution, i.e. will
-- always should be executing with --threads > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()
   print()
   create_table(drv, con, sysbench.opt.config_json)
end


-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

function get_c_value()
   return sysbench.rand.string(c_value_template)
end

function get_pad_value()
   return sysbench.rand.string(pad_value_template)
end

function create_table(drv, con, json_file)
	local table = load_json_file(json_file)
   local sql = load_sql_file(table.scheme)
   for k=1,#sql do
      con:query(sql[k])
   end
	for i=1,#table.data do 
		for j=1,table.data[i].nums do
         local line = string.gsub(table.data[i].insert,"{n}",j)
         print(line)
			con:query(line)
		end
	end
end

local t = sysbench.sql.type


function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
end

-- Close prepared statements
function close_statements()
   if (stmt.begin ~= nil) then
      stmt.begin:close()
   end
   if (stmt.commit ~= nil) then
      stmt.commit:close()
   end
end

function thread_done()
   close_statements()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping table 'sbtest%d'...", i))
      con:query("DROP TABLE IF EXISTS sbtest" .. i )
   end
end

function begin()
   stmt.begin:execute()
end

function commit()
   stmt.commit:execute()
end


-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
      errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
      errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
      errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
   then
      close_statements()
      prepare_statements()
   end
end

function load_sql_file(filename)
   local sql = read_file(filename)
   return sql.split(sql,";")
end

function load_json_file(filename)
   pathtest = string.match(test, "(.*/)")
   local json = dofile(pathtest .. "json.lua")
   local contents = read_file(filename)
   return json.decode(contents)
end

function read_file(file)
   local f = assert(io.open(file, "rb"))
   local content = f:read("*all")
   f:close()
   return content
end

function string:split(sep)
   local sep, fields = sep or ":", {}
   local pattern = string.format("([^%s]+)", sep)
   self:gsub(pattern, function(c) fields[#fields+1] = c end)
   return fields
end
