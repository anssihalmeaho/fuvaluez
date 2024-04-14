
ns main

import valuez
import stddbc

run-viewer = proc(ch1 ch2 ch3 col)
	viewer = proc(txn)
		print('In view')
		send(ch1 'ready')
		recv(ch2)

		items = call(valuez.get-values txn func(x) true end)
		print('View sees still these: ' items)

		send(ch3 'done')
		'whatever return value view'
	end

	spawn( print('view result: ' call(valuez.view col viewer) ) )
end

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample'):
	call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'examplecol'):
	call(stddbc.assert col-ok col-err)

	call(valuez.put-value col 'Pizza')
	call(valuez.put-value col 'Burger')

	ch1 ch2 ch3 = list(chan() chan() chan()):
	call(run-viewer ch1 ch2 ch3 col)
	recv(ch1)

	print('put more items to collection')
	call(valuez.put-value col 'Lasagne')
	call(valuez.put-value col 'Hot Dog')

	items = call(valuez.get-values col func(x) true end)
	print('Collection now: ' items)
	send(ch2 'continue')

	recv(ch3)
	call(valuez.close db)
	items
end

endns

