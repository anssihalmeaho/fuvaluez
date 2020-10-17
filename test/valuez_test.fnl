
ns main

import valuez
import stdfu
import stddbc
import stdfilu
import stdfiles

test-1 = proc(arguments)
	test-ok db col = arguments:
	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Ice cream')
	_ = call(valuez.put-value col 'Coffee')

	_ = call(valuez.update col func(x) if(eq(x 'Pizza') list(true 'Two Pizza') list(false '')) end)

	all-items = call(valuez.get-values col func(x) true end)
	check-1 = and(
		eq(len(all-items) 3)
		in(all-items 'Two Pizza')
		in(all-items 'Ice cream')
		in(all-items 'Coffee')
	)

	one-item = call(valuez.get-values col func(x) eq(x 'Coffee') end)
	check-2 = and(
		eq(one-item list('Coffee'))
		true
	)

	taken = call(valuez.take-values col func(x) in(list('Coffee' 'Two Pizza') x) end)
	left = call(valuez.get-values col func(x) true end)
	check-3 = and(
		in(taken 'Coffee')
		in(taken 'Two Pizza')
		eq(left list('Ice cream'))
	)

	check-result = and(
		check-1
		check-2
		check-3
	)
	list(and(check-result true) db col)
end

test-2 = proc(arguments)
	test-ok db col = arguments:

	get-trans-action = func(do-commit)
		proc(txn)
			_ = call(valuez.put-value txn 'Candy')
			_ = call(valuez.put-value txn 'Cola')

			values-1 = call(valuez.get-values txn func(x) true end)
			values-1-ok = and(
				eq(len(values-1) 3)
				in(values-1 'Ice cream')
				in(values-1 'Candy')
				in(values-1 'Cola')
			)
			_ = call(stddbc.assert values-1-ok plus('wrong values: ' str(values-1)))

			_ = call(valuez.update txn func(x) if(eq(x 'Cola') list(true 'Dr Pepper') list(false '')) end)

			values-2 = call(valuez.get-values txn func(x) true end)
			values-2-ok = and(
				eq(len(values-1) 3)
				in(values-2 'Ice cream')
				in(values-2 'Candy')
				in(values-2 'Dr Pepper')
			)
			_ = call(stddbc.assert values-2-ok plus('wrong values: ' str(values-2)))

			taken = call(valuez.take-values txn func(x) in(list('Dr Pepper' 'Ice cream') x) end)
			values-3 = call(valuez.get-values txn func(x) true end)
			values-3-ok = and(
				eq(values-3 list('Candy'))
				true
			)
			_ = call(stddbc.assert values-3-ok plus('wrong values: ' str(values-3)))

			do-commit
		end
	end

	_ = call(valuez.trans col call(get-trans-action false))
	values-after-cancel = call(valuez.get-values col func(x) true end)
	check-1 = eq(values-after-cancel list('Ice cream'))

	_ = call(valuez.trans col call(get-trans-action true))
	values-after-commit = call(valuez.get-values col func(x) true end)
	check-2 = eq(values-after-commit list('Candy'))

	check-result = and(
		check-1
		check-2
		true
	)

	list(and(check-result true) db col)
end

clean-db-file = proc(filename)
	files = call(stdfilu.get-files-by-ext '.' 'db')
	targetfile = plus(filename '.db')
	if( in(files targetfile)
		call(stdfiles.remove targetfile)
		'no file'
	)
end

main = proc()
	open-ok open-err db = call(valuez.open 'testdb'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'test-col'):
	_ = call(stddbc.assert col-ok col-err)

	testlist = list(
		test-1
		test-2
	)
	all-tests = call(stdfu.proc-pipe testlist)

	test-result = try(call(all-tests list(true db col)))

	_ = call(valuez.close db)
	_ = call(clean-db-file 'testdb')
	test-result
end

endns

