
ns main

import valuez
import stddbc

run-viewer = proc(ch1 ch2 ch3 col)
	viewer = proc(txn)
		_ = print('In view')
		_ = send(ch1 'ready')
		_ = recv(ch2)

		items = call(valuez.get-values txn func(x) true end)
		_ = print('View sees still these: ' items)

		_ = send(ch3 'done')
		'whatever return value view'
	end

	spawn( print('view result: ' call(valuez.view col viewer) ) )
end

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'examplecol'):
	_ = call(stddbc.assert col-ok col-err)

	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Burger')

	ch1 ch2 ch3 = list(chan() chan() chan()):
	_ = call(run-viewer ch1 ch2 ch3 col)
	_ = recv(ch1)

	_ = print('put more items to collection')
	_ = call(valuez.put-value col 'Lasagne')
	_ = call(valuez.put-value col 'Hot Dog')

	items = call(valuez.get-values col func(x) true end)
	_ = print('Collection now: ' items)
	_ = send(ch2 'continue')

	_ = recv(ch3)
	_ = call(valuez.close db)
	items
end

endns

