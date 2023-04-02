-- 1. Создайте таблицу users_old, аналогичную таблице users. Создайте процедуру,  с помощью которой можно переместить любого (одного) пользователя из таблицы users в таблицу users_old. (использование транзакции с выбором commit или rollback – обязательно).

CREATE TABLE users_old (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  password TEXT NOT NULL
);

CREATE OR REPLACE PROCEDURE move_user_to_old_table(user_id INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
  -- начало транзакции
  BEGIN;
  
  -- выборка пользователя из таблицы users
  SELECT * INTO temp_user FROM users WHERE id = user_id FOR UPDATE;
  
  -- вставка пользователя в таблицу users_old
  INSERT INTO users_old (name, email, password)
  VALUES (temp_user.name, temp_user.email, temp_user.password);
  
  -- удаление пользователя из таблицы users
  DELETE FROM users WHERE id = user_id;
  
  -- фиксация изменений
  COMMIT;
  
  -- очистка временной таблицы
  DROP TABLE IF EXISTS temp_user;
  
  -- вывод сообщения об успешном выполнении операции
  RAISE NOTICE 'Пользователь % был перемещен в таблицу users_old', user_id;
  
EXCEPTION
  -- обработка ошибок
  WHEN OTHERS THEN
    -- отмена транзакции
    ROLLBACK;
    
    -- вывод сообщения об ошибке
    RAISE EXCEPTION 'Ошибка перемещения пользователя % в таблицу users_old: %', user_id, SQLERRM;
    
END;
$$;


-- 2. Создайте хранимую функцию hello(), которая будет возвращать приветствие, в зависимости от текущего времени суток. С 6:00 до 12:00 функция должна возвращать фразу "Доброе утро", с 12:00 до 18:00 функция должна возвращать фразу "Добрый день", с 18:00 до 00:00 — "Добрый вечер", с 00:00 до 6:00 — "Доброй ночи".

CREATE FUNCTION hello() RETURNS VARCHAR(20)
BEGIN
  DECLARE current_time TIME;
  SET current_time = CURTIME();

  IF (current_time >= '06:00:00' AND current_time < '12:00:00') THEN
    RETURN 'Доброе утро';
  ELSEIF (current_time >= '12:00:00' AND current_time < '18:00:00') THEN
    RETURN 'Добрый день';
  ELSEIF (current_time >= '18:00:00' AND current_time < '00:00:00') THEN
    RETURN 'Добрый вечер';
  ELSE
    RETURN 'Доброй ночи';
  END IF;
END;

-- 3. Создайте таблицу logs типа Archive. Пусть при каждом создании записи в таблицах users, communities и messages в таблицу logs помещается время и дата создания записи, название таблицы, идентификатор первичного ключа.

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(50),
    record_id INTEGER
) TABLESPACE pg_global;

CREATE TRIGGER users_insert_trigger
AFTER INSERT ON users
FOR EACH ROW
EXECUTE FUNCTION insert_log('users', NEW.id);

CREATE TRIGGER communities_insert_trigger
AFTER INSERT ON communities
FOR EACH ROW
EXECUTE FUNCTION insert_log('communities', NEW.id);

CREATE TRIGGER messages_insert_trigger
AFTER INSERT ON messages
FOR EACH ROW
EXECUTE FUNCTION insert_log('messages', NEW.id);

CREATE OR REPLACE FUNCTION insert_log(table_name VARCHAR, record_id INTEGER)
RETURNS void AS $$
BEGIN
    INSERT INTO logs (table_name, record_id) VALUES (table_name, record_id);
END;
$$ LANGUAGE plpgsql;

-- 4. Создайте функцию, которая принимает кол-во сек и формат их в кол-во дней часов. Пример: 123456 ->'1 days 10 hours 17 minutes 36 seconds '

CREATE OR REPLACE FUNCTION format_seconds(
    num_seconds INTEGER,
    OUT formatted_string TEXT
) AS $$
DECLARE
    num_days INTEGER;
    num_hours INTEGER;
    num_minutes INTEGER;
    num_remaining_seconds INTEGER;
BEGIN
    num_days := num_seconds / 86400;
    num_remaining_seconds := num_seconds % 86400;
    num_hours := num_remaining_seconds / 3600;
    num_remaining_seconds := num_remaining_seconds % 3600;
    num_minutes := num_remaining_seconds / 60;
    num_remaining_seconds := num_remaining_seconds % 60;

    formatted_string := num_days || ' days ' || num_hours || ' hours ' || num_minutes || ' minutes ' || num_remaining_seconds || ' seconds';
END;
$$ LANGUAGE plpgsql;

SELECT format_seconds(987654321);

-- 5. Выведите только четные числа от 1 до 10. Пример: 2,4,6,8,10

CREATE OR REPLACE FUNCTION even_numbers()
  RETURNS TABLE (number INT) AS
$$
BEGIN
  FOR i IN 1..10 LOOP
    IF i % 2 = 0 THEN
      number := i;
      RETURN NEXT;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM even_numbers();
