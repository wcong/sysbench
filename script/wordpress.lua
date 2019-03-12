#!/usr/bin/env sysbench

pathtest = string.match(test, "(.*/)")

if pathtest then
	dofile(pathtest .. "common.lua")
else
	require("common")
end

function event()
	print("start")
	local table = load_json_file(sysbench.opt.config_json)
	local pages = #table.pages
	for i = 1,pages,1 
	do
		print("execute page:" .. table.pages[i].page)
		local sql_list = table.pages[i].sql
		for j=1,#sql_list do
			con:query(sql_list[j])
		end
	end
end
