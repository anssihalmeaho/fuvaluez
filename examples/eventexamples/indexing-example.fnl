ns main

import valuez
import stddbc

new-movies-cache = proc()
	import lens
	import stdvar

	cache = call(stdvar.new map(
		'by-director' map()
		'by-year'     map()
	))

	add-by-key = func(prev path val)
		found _ = call(lens.get-from path prev):
		if(found
			call(lens.apply-to path prev func(lst) append(lst val) end)
			call(lens.set-to path prev list(val))
		)
	end

	listener = proc(event)
		event-type movie-list = event:
		call(stdvar.change cache func(prev)
			case(event-type
				'added'
				call(func()
					movie = head(movie-list)
					director = get(movie 'Director')
					year = get(movie 'Year')

					nextv = call(add-by-key prev list('by-director' director) movie)
					call(add-by-key nextv list('by-year' year) movie)
				end)

				prev
			)
		end)
	end

	value = proc()
		call(stdvar.value cache)
	end

	get-by-director = proc(director)
		v = call(stdvar.value cache)
		call(lens.get-from list('by-director' director) v)
	end

	get-by-year = proc(year)
		v = call(stdvar.value cache)
		call(lens.get-from list('by-year' year) v)
	end

	map(
		'listener'        listener
		'get-by-director' get-by-director
		'get-by-year'     get-by-year
		'value'           value
	)
end

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample' map('in-mem' true)):
	call(stddbc.assert open-ok open-err)
	col-ok col-err col = call(valuez.new-col db 'movies'):
	call(stddbc.assert col-ok col-err)

	ob = call(new-movies-cache)
	listener = get(ob 'listener')
	get-by-director = get(ob 'get-by-director')
	get-by-year = get(ob 'get-by-year')
	value = get(ob 'value')

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
		'Title'     'The Straight Story'
		'Year'      1999
	))
	call(valuez.put-value col map(
		'Director'  list('David' 'Lynch')
		'Title'     'Blue Velvet'
		'Year'      1986
	))


	import stdpp
	call(stdpp.form list(
		'Movies by David Lynch'
		call(get-by-director list('David' 'Lynch'))

		'Movies by year 1999'
		call(get-by-year 1999)
	))
end

endns

