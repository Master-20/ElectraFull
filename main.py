import numbers

import psycopg2
import prettytable

exit_btn = ""
print("Добро пожаловать в СУБД магазина электротоваров!")
print("Авторизуйтесь для продолжения работы в системе:")
DbName = input('Название БД: ')
DbPass = input('Пароль: ')
string1 = "Авторизация в системе прошла успешно."
string2 = "Вам доступны следующие операции: Просмотр таблицы > 1; Добавление записи > 2;  Редактирование записи > 3; Удаление записи > 4."
string3 = "Вы можете выполнить следующие аналитические запросы: Показать среднее количество покупателей за день > 5;"
string4 = "Показать должности, занимающие наименьшее количество сотрудников > 6; Показать средний чек заказа > 7;"
string5 = "Узнать, товары каких поставщиков представлены в наибольшем количестве в магазине > 8."
while (exit_btn != "1"):
    try:
        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            database=f"{DbName}",
            password=f"{DbPass}",
        )
        print(connection)
        print("-" * len(string2))
        print(string1)
        print("-" * len(string2))
        print(string2)
        print("-" * len(string2))
        print(string3)
        print(string4)
        print(string5)
        print("-" * len(string2))
        op_type = input("Выберите операцию: ")
        try:
            if op_type == "1":
                with connection.cursor() as cursor:
                    table_name = input("Выберите таблицу для просмотра (Электротовар, Заказ, Сотрудник): ")
                    if table_name == 'Электротовар':
                        cursor.execute("SELECT i.Код_товара AS id_item, i.Наименование AS item_name, "
                                       "i.Тип_товара AS category_name, i.Стоимость AS item_price, "
                                       "i.Тип_цоколя_разъема AS item_connector, i.№_стеллажа_полки AS id_stillage_shelf, "
                                       "pe.Код_поставщика AS id_provider, p.Название AS provider_name "
                                       "FROM Электротовар i "
                                       "LEFT JOIN Поставщик_Электротовар pe ON i.Код_товара = pe.Код_товара "
                                       "LEFT JOIN Поставщик p ON pe.Код_поставщика = p.Код_поставщика "
                                       "ORDER BY i.Код_товара;")
                    elif table_name == 'Заказ':
                        cursor.execute("SELECT o.№_заказа AS id_order, o.Дата_заказа AS order_date, "
                                       "o.Состав_заказа AS order_list, o.Стоимость AS order_price, "
                                       "o.Способ_оплаты AS payment_method, o.Контакты_заказчика AS contacts_customer, "
                                       "o.№_курьера AS id_courier, d.Тип_доставки AS shipping_method, "
                                       "d.Транспорт AS transport "
                                       "FROM Заказ o "
                                       "LEFT JOIN Служба_доставки d ON o.№_курьера = d.№_курьера;")
                    elif table_name == 'Сотрудник':
                        cursor.execute("SELECT e.№_сотрудника AS id_employee, e.ФИО AS employee_fio, "
                                       "e.Пол AS employee_gender, e.Возраст AS employee_age, "
                                       "e.Образование AS employee_education, e.Опыт_работы AS employee_experience, "
                                       "e.График_работы AS employee_schedule, e.Контакты AS employee_contacts, "
                                       "e.Название_должности AS job_title, j.Оклад AS employee_salary, "
                                       "j.Права AS job_rights, j.Обязанности AS job_responsibilities "
                                       "FROM Сотрудник e "
                                       "LEFT JOIN Должность j ON e.Название_должности = j.Название_должности;")
                    else:
                        cursor.execute(f"SELECT * FROM {table_name}")
                    table_data = prettytable.from_db_cursor(cursor)
                    print(table_data)
                    print("Количество записей:", len(table_data.rows))
                exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
            elif op_type == "2":
                #Добавление записи
                with connection.cursor() as cursor:
                    #Получение данных из таблицы
                    table_name = input("Введите название таблицы для добавления в нее записи: ")
                    cursor.execute(f"SELECT column_name as Column, data_type as Data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
                    table_data = prettytable.from_db_cursor(cursor)
                    data_places = []
                    data_places_str = ''
                    for i in range(len(table_data.rows)):
                        data_places.append(table_data.rows[i][0])
                    for i in range(len(data_places)):
                        if i != len(data_places)-1:
                            data_places_str += data_places[i] + ','
                        else:
                            data_places_str += data_places[i]
                    print("Колонки вашей таблицы и их типы данных: ")
                    print(table_data)
                    #Ввод новых данных
                    data = input('Введите новые данные через ";". Пример:\nзначение1; значение2; ...\n')
                    data = data.split('; ')
                    #Преобразование данных типа int к данным типа int
                    #(фактическое преобразование вместо string, даты и строки не затронуты, на float не тестил)
                    for i in range(len(data)):
                        try:
                            if not (data[i].startswith('+') or data[i].__contains__('-')):
                                data[i] = int(data[i])
                        except:
                            pass
                    #Преобразование к кортежу и комит в БД
                    data_final = tuple(data)
                    cursor.execute(f"INSERT INTO {table_name}({data_places_str}) VALUES {data_final}")
                    connection.commit()
                    print("Добавление данных завершено")
                    print('-' * len(string2))
                    exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
            elif op_type == "3":
                with connection.cursor() as cursor:
                    print('-' * len(string2))
                    table_name = input("Введите название таблицы, которую требуется отредактировать: ")
                    # забираем из БД названия и типы данных колонок
                    cursor.execute("SELECT "
                                   "column_name as Column, "
                                   "data_type as Data_type "
                                   "FROM information_schema.columns "
                                   f"WHERE table_name = '{table_name}'")
                    table_data = prettytable.from_db_cursor(cursor)
                    # удаляем ряд, в котором содержится id (PK таблицы)
                    table_data.del_row(0)
                    print("Колонки вашей таблицы и их типы данных:")
                    print(table_data)
                    if table_name == 'Должность': id_table = 'Название_должности'
                    elif table_name == 'Сотрудник': id_table = '№_сотрудника'
                    elif table_name == 'Касса': id_table = '№_кассы'
                    elif table_name == 'Покупатель ': id_table = '№_покупки'
                    elif table_name == 'Служба_доставки': id_table = '№_курьера'
                    elif table_name == 'Заказ': id_table = '№_заказа'
                    elif table_name == 'Стенд_проверки': id_table = '№_цоколя_разъема'
                    elif table_name == 'Склад': id_table = '№_стеллажа_полки'
                    elif table_name == 'Электротовар': id_table = 'Код_товара'
                    elif table_name == 'Поставщик': id_table = 'Код_поставщика'
                    elif table_name == 'Заказ_Электротовар': id_table = '№_заказа'
                    elif table_name == 'Покупатель_Электротовар': id_table = '№_покупки'
                    elif table_name == 'Поставщик_Электротовар': id_table = 'Код_поставщика'
                    choice_redact = input("Необходимо отредактировать только конкретное значение или всю строку? "
                                          "(Вся строка > 1, Конкретный столбец > 2) ")
                    if choice_redact == '1':
                        # получаем новые данные, делим их по ; потому что так проще всего поделить
                        data = input('Введите новые данные через ";". Пример:\nзначение1; значение2; ...\n')
                        print('-' * len(string2))
                        data = data.split('; ')
                        # преобразование данных типа int к данным типа int
                        # (фактическое преобразование вместо string, даты и строки не затронуты, на float не тестил)
                        # альтернатива - сделать как ниже, сравнить с типом данных из таблицы (table_data)
                        for i in range(len(data)):
                            try:
                                if not (data[i].startswith('+') or data[i].__contains__('-')):
                                    data[i] = int(data[i])
                            except:
                                pass

                        # преобразование к кортежу и комит в БД
                        data_final = tuple(data)
                        id_redacted = int(input("Введите ID записи, которую требуется изменить: "))
                        print('-' * len(string2))
                        choice_choice = input("Вы уверены, что хотите изменить эти данные? (Да > 1, Нет > 2) ")
                        print('-' * len(string2))
                        set_string = ""
                        if choice_choice == '1':
                            # получение строки ввода в бд (всё не int-овое или float-овое должно быть в кавычках ' ')
                            # общий принцип - в строку добавляется элемент кортежа,
                            # в зависимости от типа данных под столбец он вводится с кавычками или без
                            for i in range(len(data)):
                                if i != len(data) - 1:
                                    if table_data.rows[i][1] == 'integer':
                                        set_string += f"{table_data.rows[i][0]} = {data_final[i]}, "
                                    else:
                                        set_string += f"{table_data.rows[i][0]} = '{data_final[i]}', "
                                else:
                                    if table_data.rows[i][1] == 'integer':
                                        set_string += f"{table_data.rows[i][0]} = {data_final[i]} "
                                    else:
                                        set_string += f"{table_data.rows[i][0]} = '{data_final[i]}' "

                            cursor.execute(f"UPDATE {table_name} SET {set_string} WHERE {id_table} = {id_redacted}")
                            connection.commit()
                            print("Обновление данных завершено")
                            print('-' * len(string2))
                    else:
                        choice_redact = input("Какой столбец требуется отредактировать? "
                                              "(введите название с учетом регистра): ")
                        choice_id = input("Введите ID записи, которую требуется отредактировать: ")
                        data = input("Введите новые данные: ")
                        # если на входе подразумевается строка или дата, то
                        try:
                            if data.startswith('+') or data.__contains__('-') or (not any(chr.isdigit() for chr in data)):
                                data = f"'{data}'"
                        except:
                            pass
                        cursor.execute(f"UPDATE {table_name} SET {choice_redact} = {data} WHERE {id_table} = {choice_id}")
                        connection.commit()
                        print("Обновление данных завершено")
                    exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
                    del choice_redact
            elif op_type == "4":
                with connection.cursor() as cursor:
                    table_name = input("Введите название таблицы, запись в которой требуется удалить: ")
                    cursor.execute(f"SELECT * from {table_name}")
                    rows = cursor.fetchall()
                    count = 0
                    column_names = [col[0] for col in cursor.description]
                    print("-" * len(string2))
                    print("Колонки в выбранной таблице: " + (" " * 5).join(column_names))
                    print("-" * len(string2))
                    record_id = input("Введите ID записи, которую требуется удалить: ")
                    print('-' * len(string2))
                    del_approve = input("Вы уверены, что хотите удалить данную запись? (Да > 1, Нет > 2) ")
                    print('-' * len(string2))
                    if del_approve == "1":
                        if table_name == 'Сотрудник':
                            cursor.execute(f"DELETE FROM Сотрудник WHERE №_сотрудника = {record_id};")
                            cursor.execute(f"DELETE FROM Касса WHERE №_сотрудника = {record_id};")
                            #DONE
                        elif table_name == 'Касса':
                            cursor.execute(f"DELETE FROM Касса WHERE №_кассы = {record_id};")
                            #DONE
                        elif table_name == 'Покупатель':
                            cursor.execute(f"DELETE FROM Покупатель WHERE №_покупки = {record_id};")
                            cursor.execute(f"DELETE FROM Покупатель_Электротовар WHERE №_покупки = {record_id};")
                            #DONE
                        elif table_name == 'Служба_доставки':
                            cursor.execute(f"DELETE FROM Служба_доставки WHERE №_курьера = {record_id};")
                            #DONE
                        elif table_name == 'Заказ':
                            cursor.execute(f"DELETE FROM Заказ WHERE №_заказа = {record_id};")
                            cursor.execute(f"DELETE FROM Заказ_Электротовар WHERE №_заказа = {record_id};")
                            #DONE
                        elif table_name == 'Стенд_проверки':
                            cursor.execute(f"DELETE FROM Стенд_проверки WHERE Тип_цоколя_разъема = {record_id};")
                            #DONE
                        elif table_name == 'Склад':
                            cursor.execute(f"DELETE FROM Склад WHERE №_стеллажа_полки = {record_id};")
                            #DONE
                        elif table_name == 'Электротовар':
                            cursor.execute(f"DELETE FROM Электротовар WHERE Код_товара = {record_id};")
                            cursor.execute(f"DELETE FROM Заказ_Электротовар WHERE Код_товара = {record_id};")
                            cursor.execute(f"DELETE FROM Покупатель_Электротовар WHERE Код_товара = {record_id};")
                            cursor.execute(f"DELETE FROM Поставщик_Электротовар WHERE Код_товара = {record_id};")
                            #DONE
                        elif table_name == 'Поставщик':
                            cursor.execute(f"DELETE FROM Поставщик WHERE Код_поставщика = {record_id};")
                            cursor.execute(f"DELETE FROM Поставщик_Электротовар WHERE Код_поставщика = {record_id};")
                            #DONE
                        elif table_name == 'Должность':
                            cursor.execute(f"DELETE FROM Должность WHERE Название_должности = {record_id};")
                            #DONE
                        elif table_name == 'Заказ_Электротовар':
                            choice = input("Если Вы ввели №_заказа, нажмите 1; Если Вы ввели Код_товара, нажмите 2: ")
                            if choice == '1':
                                choice1 = input("Если Вы желаете осуществить удаление записи по двум индекам, нажмите 1, иначе 2: ")
                                if choice == '1':
                                    record_id1 = input("Введите Код_товара: ")
                                    cursor.execute(f"DELETE FROM Заказ_Электротовар WHERE №_заказа = {record_id} AND Код_товара = {record_id1};")
                                else:
                                    cursor.execute(f"DELETE FROM Заказ_Электротовар WHERE №_заказа = {record_id};")
                                del choice, choice1
                            else:
                                choice1 = input("Если Вы желаете осуществить удаление записи по двум индекам, нажмите 1, иначе 2: ")
                                if choice == '1':
                                    record_id1 = input("Введите №_заказа: ")
                                    cursor.execute(f"DELETE FROM Заказ_Электротовар WHERE Код_товара = {record_id} AND №_заказа = {record_id1};")
                                else:
                                    cursor.execute(f"DELETE FROM Заказ_Электротовар WHERE Код_товара = {record_id};")
                                del choice, choice1
                            #DONE
                        elif table_name == 'Покупатель_Электротовар':
                            choice = input("Если Вы ввели №_покупки, нажмите 1; Если Вы ввели Код_товара, нажмите 2: ")
                            if choice == '1':
                                choice1 = input("Если Вы желаете осуществить удаление записи по двум индекам, нажмите 1, иначе 2: ")
                                if choice == '1':
                                    record_id1 = input("Введите Код_товара: ")
                                    cursor.execute(f"DELETE FROM Покупатель_Электротовар WHERE №_покупки = {record_id} AND Код_товара = {record_id1};")
                                else:
                                    cursor.execute(f"DELETE FROM Покупатель_Электротовар WHERE №_покупки = {record_id};")
                                del choice, choice1
                            else:
                                choice1 = input("Если Вы желаете осуществить удаление записи по двум индекам, нажмите 1, иначе 2: ")
                                if choice == '1':
                                    record_id1 = input("Введите №_покупки: ")
                                    cursor.execute(f"DELETE FROM Покупатель_Электротовар WHERE Код_товара = {record_id} AND №_покупки = {record_id1};")
                                else:
                                    cursor.execute(f"DELETE FROM Покупатель_Электротовар WHERE Код_товара = {record_id};")
                                del choice, choice1
                            #DONE
                        elif table_name == 'Поставщик_Электротовар':
                            choice = input("Если Вы ввели №_поставщика, нажмите 1; Если Вы ввели Код_товара, нажмите 2: ")
                            if choice == '1':
                                choice1 = input("Если Вы желаете осуществить удаление записи по двум индекам, нажмите 1, иначе 2: ")
                                if choice == '1':
                                    record_id1 = input("Введите Код_товара: ")
                                    cursor.execute(f"DELETE FROM Поставщик_Электротовар WHERE №_поставщика = {record_id} AND Код_товара = {record_id1};")
                                else:
                                    cursor.execute(f"DELETE FROM Поставщик_Электротовар WHERE №_поставщика = {record_id};")
                                del choice, choice1
                            else:
                                choice1 = input("Если Вы желаете осуществить удаление записи по двум индекам, нажмите 1, иначе 2: ")
                                if choice == '1':
                                    record_id1 = input("Введите №_поставщика: ")
                                    cursor.execute(f"DELETE FROM Поставщик_Электротовар WHERE Код_товара = {record_id} AND №_поставщика = {record_id1};")
                                else:
                                    cursor.execute(f"DELETE FROM Поставщик_Электротовар WHERE Код_товара = {record_id};")
                                del choice, choice1
                            #DONE
                        connection.commit()
                        print("Удаление данных завершено")
                        print('-' * len(string2))
                    exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
            elif op_type == "5":
                print("Среднее количество покупателей за день:\n")
                with connection.cursor() as cursor:
                    cursor.execute("SELECT AVG(Number_buyers) "
                                   "AS Average_number_buys "
                                   "FROM (SELECT COUNT(*) "
                                   "AS Number_buyers FROM Покупатель "
                                   "GROUP BY Дата_покупки) "
                                   "AS Buys;")
                    table_data = prettytable.from_db_cursor(cursor)
                    print(table_data)
                exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
            elif op_type == "6":
                number_jobs = input("Введите количество должностей, которое хотите увидеть: ")
                print("Должности, занимающие наименьшее количество сотрудников:\n")
                with connection.cursor() as cursor:
                    cursor.execute("SELECT Название_должности AS Job_title, COUNT(*) AS Number_employees "
                                   "FROM Сотрудник  "
                                   "GROUP BY Название_должности "
                                   "ORDER BY Number_employees "
                                   f"LIMIT {number_jobs};")
                    table_data = prettytable.from_db_cursor(cursor)
                    print(table_data)
                exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
            elif op_type == "7":
                print("Средний чек заказа:\n")
                with connection.cursor() as cursor:
                    cursor.execute("SELECT AVG(Стоимость) AS Average_summ FROM Заказ;")
                    table_data = prettytable.from_db_cursor(cursor)
                    print(table_data)
                exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
            elif op_type == "8":
                print("Поставщики, товары которых представлены в наибольшем количестве в магазине:\n")
                with connection.cursor() as cursor:
                    cursor.execute("SELECT Поставщик.Название AS Provider_name "
                                   "FROM Поставщик JOIN ( "
                                   "SELECT Поставщик_Электротовар.Код_поставщика, COUNT(*) AS Number_products "
                                   "FROM Поставщик_Электротовар "
                                   "JOIN Электротовар ON Поставщик_Электротовар.Код_товара = Электротовар.Код_товара "
                                   "GROUP BY Поставщик_Электротовар.Код_поставщика HAVING COUNT(*) = ( "
                                   "SELECT COUNT(*) AS Number_products "
                                   "FROM Поставщик_Электротовар "
                                   "JOIN Электротовар ON Поставщик_Электротовар.Код_товара = Электротовар.Код_товара "
                                   "GROUP BY Поставщик_Электротовар.Код_поставщика "
                                   "ORDER BY Number_products DESC "
                                   "LIMIT 1)) AS Products_providers "
                                   "ON Поставщик.Код_поставщика = Products_providers.Код_поставщика;")
                    table_data = prettytable.from_db_cursor(cursor)
                    print(table_data)
                exit_btn = input("Для выхода нажмите 1; Для выбора операции нажмите 2: ")
        finally:
            connection.close()
    except psycopg2.Error as e:
        print(e)