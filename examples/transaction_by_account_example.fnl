
ns main

import valuez
import stddbc

example-transaction = proc(txn)
	# move 250 euros from John to Jack in consistent way
	transfer-amount = 250

	john-old = head(call(valuez.take-values txn
		func(x)
			eq(get(x 'name') 'John')
		end
	))
	john-new = map(
		'name'  get(john-old 'name')
		'saldo' minus(get(john-old 'saldo') transfer-amount)
	)
	jack-old = head(call(valuez.take-values txn
		func(x)
			eq(get(x 'name') 'Jack')
		end
	))
	jack-new = map(
		'name'  get(jack-old 'name')
		'saldo' plus(get(jack-old 'saldo') transfer-amount)
	)

	_ = call(valuez.put-value txn john-new)
	_ = call(valuez.put-value txn jack-new)

	items = call(valuez.get-values txn func(x) true end)
	_ = print('From transaction accounts are: ' items '\n')

	true # changes are committed
	#false # changes are not committed
end

main = proc()
	open-ok open-err db = call(valuez.open 'dbexample'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'accounts'):
	_ = call(stddbc.assert col-ok col-err)

	# saldos for each person
	_ = call(valuez.put-value col map('name' 'John' 'saldo' 3500))
	_ = call(valuez.put-value col map('name' 'Jack' 'saldo' 1000))
	_ = call(valuez.put-value col map('name' 'Steve' 'saldo' 500))
	_ = print('orinally accounts are: ' call(valuez.get-values col func(x) true end) '\n')

	_ = call(valuez.trans col example-transaction)
	items = call(valuez.get-values col func(x) true end)

	_ = call(valuez.close db)
	sprintf('resulting accounts are: %v' items)
end

endns

