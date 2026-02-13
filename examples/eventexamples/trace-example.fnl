ns main

import valuez
import stddbc
import stdstr

new-ob = proc()
	import stdvar
	event-log = call(stdvar.new list())

	map(
		'listener'
		proc(event)
			call(stdvar.change event-log func(prev) append(prev event) end)
		end

		'trace'
		proc()
			call(stdvar.value event-log)
		end
	)
end

example-transaction = proc(txn)
	call(valuez.put-value txn map(
		'Director'  list('Stanley' 'Kubrick')
		'Title'     'Full Metal Jacket'
		'Year'      1987
	))
	call(valuez.put-value txn map(
		'Director'  list('David' 'Lynch')
		'Title'     'The Straight Story'
		'Year'      1999
	))
	call(valuez.update txn func(movie)
		change = eq(get(movie 'Title') 'Eyes Wide Shut')
		if(change
			list(true put(movie 'Actors' list('Nicole Kidman' 'Tom Cruise')))
			list(false movie)
		)
	end)
	call(valuez.take-values txn func(movie)
		eq(get(movie 'Title') 'Paths of Glory')
	end)
	true
end

open-col = proc(db colname)
	found _ col1 = call(valuez.get-col db colname):
	if(found
		col1
		call(proc()
			col-ok col-err col2 = call(valuez.new-col db colname):
			call(stddbc.assert col-ok col-err)
			col2
		end)
	)
end

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample' map('in-mem' true)):
	call(stddbc.assert open-ok open-err)
	col = call(open-col db 'movies')

	ob = call(new-ob)
	listener = get(ob 'listener')
	trace = get(ob 'trace')

	call(valuez.add-listener col listener)

	call(valuez.put-value col map(
		'Director'  list('Stanley' 'Kubrick')
		'Title'     'Eyes Wide Shut'
		'Year'      1999
	))
	call(valuez.put-value col map(
		'Director'  list('Stanley' 'Kubrick')
		'Title'     'Paths of Glory'
		'Year'      1957
	))
	call(valuez.put-value col map(
		'Director'  list('David' 'Lynch')
		'Title'     'Eraserhead'
		'Year'      1977
	))
	call(valuez.put-value col map(
		'Director'  list('David' 'Lynch')
		'Title'     'Blue Velvet'
		'Year'      1986
	))

	call(valuez.take-values col func(movie)
		eq(get(movie 'Title') 'Eraserhead')
	end)
	call(valuez.update col func(movie)
		change = eq(get(movie 'Title') 'Blue Velvet')
		list(change put(movie 'Actors' list('Laura Dern')))
	end)
	call(valuez.trans col example-transaction)

	import stdpp
	call(stdpp.form call(trace))
end

endns

