
ns main

import valuez
import stddbc

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample' map('in-mem' true)):
	call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'fastfood'):
	call(stddbc.assert col-ok col-err)

	call(valuez.put-value col 'Pizza')
	call(valuez.put-value col 'Burger')
	call(valuez.put-value col 'Hot Dog')

	call(valuez.get-values col func(x) true end)
end

endns


