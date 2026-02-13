
ns main

import valuez
import stddbc
import stdpp
import stdstr

# opens db and collection
make-col = proc()
	open-ok open-err db = call(valuez.open 'dbexample' map('in-mem' true)):
	call(stddbc.assert open-ok open-err)
	col-ok col-err col = call(valuez.new-col db 'fastfood'):
	call(stddbc.assert col-ok col-err)
	col
end

# recorder is object which listens events and stores those
new-recorder = proc()
	import stdvar
	var = call(stdvar.new list())
	map(
		'listener'
		proc(item)
			call(stdvar.change var func(prev) append(prev item) end)
		end

		'trace'
		proc() call(stdvar.value var) end
	)
end

# test basic atomic operation event log
test-basic = proc()
	recorder = call(new-recorder)
	listener = get(recorder 'listener')
	trace = get(recorder 'trace')

	col = call(make-col)
	call(stddbc.assert call(valuez.add-listener col listener) 'add listener failed')

	call(valuez.put-value col 'A')
	call(valuez.put-value col 'B')
	call(valuez.put-value col 'C')
	call(valuez.update col func(x) list(true call(stdstr.lowercase x)) end)
	call(valuez.take-values col func(x) in(list('b' 'c') x) end)

	adds = list(
		list('added' list('A'))
		list('added' list('B'))
		list('added' list('C'))
	)
	received-trace = call(trace)
	#print(call(stdpp.form received-trace))
	a1 a2 a3 upd deleted = received-trace:

	# check additions
	call(stddbc.assert
		eq(list(a1 a2 a3) adds)
		sprintf('wrong trace: %v' call(stdpp.form received-trace))
	)

	# check updated items
	updtag updlist = upd:
	call(stddbc.assert eq(updtag 'updated') sprintf('wrong upd tag: %v' updtag))
	call(stddbc.assert eq(len(updlist) 3) sprintf('wrong upd list: %v' updlist))
	call(stddbc.assert in(updlist list('A' 'a')) sprintf('wrong update: %v' upd))
	call(stddbc.assert in(updlist list('B' 'b')) sprintf('wrong update: %v' upd))
	call(stddbc.assert in(updlist list('C' 'c')) sprintf('wrong update: %v' upd))

	# check deleted items
	deltag dellist = deleted:
	call(stddbc.assert eq(deltag 'deleted') sprintf('wrong del tag: %v' deltag))
	call(stddbc.assert eq(len(dellist) 2) sprintf('wrong del list: %v' dellist))
	call(stddbc.assert in(dellist 'b') sprintf('wrong delete: %v' deleted))
	call(stddbc.assert in(dellist 'c') sprintf('wrong delete: %v' deleted))
end

# test transaction event log
test-transaction = proc()
	recorder = call(new-recorder)
	listener = get(recorder 'listener')
	trace = get(recorder 'trace')

	col = call(make-col)
	call(stddbc.assert call(valuez.add-listener col listener) 'add listener failed')

	call(valuez.put-value col 'A')
	call(valuez.put-value col 'B')
	call(valuez.put-value col 'C')
	call(valuez.put-value col 'D')
	call(valuez.put-value col 'X')
	call(valuez.trans col proc(txn)
		call(valuez.put-value txn 'E')
		call(valuez.put-value txn 'F')
		call(valuez.put-value txn 'G')
		call(valuez.update txn func(x)
			if(in(list('A' 'G') x)
				list(true call(stdstr.lowercase x))
				list(false x)
			)
		end)
		call(valuez.update txn func(x)
			if(eq(x 'X')
				list(true call(stdstr.lowercase x))
				list(false x)
			)
		end)
		call(valuez.take-values txn func(x) in(list('B' 'C' 'g') x) end)
		true
	end)

	received-trace = call(trace)
	#print(call(stdpp.form received-trace))
	a1 a2 a3 a4 a5 trans = received-trace:

	# check additions
	adds = list(
		list('added' list('A'))
		list('added' list('B'))
		list('added' list('C'))
		list('added' list('D'))
		list('added' list('X'))
	)
	call(stddbc.assert
		eq(list(a1 a2 a3 a4 a5) adds)
		sprintf('wrong trace: %v' call(stdpp.form received-trace))
	)

	# check transaction
	transtag additions updates deletions = trans:
	call(stddbc.assert eq(transtag 'transaction') sprintf('wrong trans tag: %v' transtag))

	addtag addlist = additions:
	call(stddbc.assert eq(addtag 'added') sprintf('wrong add tag: %v' addtag))
	call(stddbc.assert eq(len(addlist) 2) sprintf('wrong add list: %v' addlist))
	call(stddbc.assert in(addlist 'E') sprintf('wrong add: %v' additions))
	call(stddbc.assert in(addlist 'F') sprintf('wrong add: %v' additions))

	updtag updlist = updates:
	call(stddbc.assert eq(updtag 'updated') sprintf('wrong upd tag: %v' updtag))
	call(stddbc.assert eq(len(updlist) 2) sprintf('wrong upd list: %v' updlist))
	call(stddbc.assert in(updlist list('A' 'a')) sprintf('wrong update: %v' updates))
	call(stddbc.assert in(updlist list('X' 'x')) sprintf('wrong update: %v' updates))

	deltag dellist = deletions:
	call(stddbc.assert eq(deltag 'deleted') sprintf('wrong del tag: %v' deltag))
	call(stddbc.assert eq(len(dellist) 2) sprintf('wrong del list: %v' dellist))
	call(stddbc.assert in(dellist 'B') sprintf('wrong delete: %v' deletions))
	call(stddbc.assert in(dellist 'C') sprintf('wrong delete: %v' deletions))
end

# run tests
main = proc()
	passed err _ = tryl(call(proc()
		call(test-basic)
		call(test-transaction)
	end)):

	if(passed
		'PASS'
		sprintf('FAIL: %s' err)
	)
end

endns

