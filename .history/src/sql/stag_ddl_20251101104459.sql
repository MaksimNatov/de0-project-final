--Создаем справочник курсов валют
CREATE TABLE IF NOT EXISTS STV202506164__STAGING.currencies (
    date_update timestamp,
    currency_code varchar(3),
    currency_code_with varchar(3),
    currency_with_div numeric(5,3)
)
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update, currency_code) ALL NODES
PARTITION BY EXTRACT(YEAR FROM date_update) * 100 + EXTRACT(MONTH FROM date_update)
;

--Создаем таблицу трансакций
CREATE TABLE IF NOT EXISTS STV202506164__STAGING.transactions (
	operation_id varchar(60),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp
)
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES
PARTITION BY EXTRACT(YEAR FROM transaction_dt) * 100 + EXTRACT(MONTH FROM transaction_dt)
;

