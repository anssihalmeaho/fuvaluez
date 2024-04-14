
ns main

import valuez
import stddbc

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample'):
	call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'examplecol'):
	call(stddbc.assert col-ok col-err)

	call(valuez.put-value col 'Pizza')
	call(valuez.put-value col 'Burger')
	call(valuez.put-value col 'Hot Dog')

	import stdstr
	call(valuez.update col func(x) list(true call(stdstr.lowercase x)) end)

	items = call(valuez.get-values col func(x) true end)

	call(valuez.close db)
	items
end

endns

