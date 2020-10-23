
ns main

import valuez
import stddbc

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'examplecol'):
	_ = call(stddbc.assert col-ok col-err)

	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Burger')
	_ = call(valuez.put-value col 'Hot Dog')

	import stdstr
	_ = call(valuez.update col func(x) list(true call(stdstr.lowercase x)) end)

	items = call(valuez.get-values col func(x) true end)

	_ = call(valuez.close db)
	items
end

endns

