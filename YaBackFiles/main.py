from flask import Flask, Response
import json
import sqlite3
from flask import request
import re

app = Flask(__name__)
con = sqlite3.connect('file.db', check_same_thread=False)
cur = con.cursor()

# Создание таблиц
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()

if not tables or ('file' not in tables[0]):
    cur.execute(
        '''
        CREATE TABLE file
        (id TEXT NOT NULL UNIQUE, type TEXT NOT NULL, url TEXT, date TEXT NOT NULL, parentId TEXT, size INTEGER, childrenCount INTEGER)
        ''')

if not tables or ('file_copy' not in tables[1]):
    cur.execute(
        '''
        CREATE TABLE file_copy
        (id TEXT NOT NULL, type TEXT NOT NULL, url TEXT, date TEXT NOT NULL, parentId TEXT, size INTEGER, childrenCount INTEGER)
        ''')

con.commit()



@app.route('/')
def hello_world():
    return 'Hello World!'


# Функция импорта данных из json формата и обработчики поступивших данных
@app.route('/imports', methods=['POST'])
def imports():

    data = request.json
    flag = True

    # Проверка валидности переданных данных
    try:
        parent_list = []
        for i in data['items']:
            # Проверка, что parentId не указывает на файл
            parent = i['parentId']
            parentId_finder = cur.execute(f'SELECT * FROM file WHERE id = "{parent}"').fetchall()

            if parentId_finder == []:
                for j in parent_list:
                    if parent == j['id']:
                        parentId_finder = j

            if parentId_finder != []:
                if type(parentId_finder) == list:
                    if parentId_finder[0][1] == 'FILE':
                        return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')

                elif type(parentId_finder) == dict:
                    if parentId_finder['type'] == 'FILE':
                        return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')
            else:
                if parent is not None:
                    print(parentId_finder, i, parent_list)
                    return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')
            flag = flag * check_id(i['id']) * check_type(i['type']) * check_time(data['updateDate'])

            if i['type'] == 'FILE':
                flag = flag * check_size(i['size'])
            elif i['type'] == 'FOLDER':
                try:
                    mistake = i['size']
                    if mistake != "" or mistake != 0:
                        flag = False
                except:
                    pass
            else:
                flag = False

            if i['type'] == "FILE":
                try:
                    flag = flag * check_url(i['url'])
                except:
                    flag = False
            parent_list.append(i)


        if not flag:
            return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')

    except:
        return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')

    # Добавление переданных данных
    for i in data['items']:
        childrenCount = 0
        id = i['id']
        id_finder = cur.execute(f'SELECT * FROM file WHERE id = "{id}"').fetchall()
        # Условие того, что данный id передан впервые
        if not id_finder:
            try:
                size = None
                try:
                    if i['size'] is not None:
                        size = i['size']
                except:
                    pass
                dd = data["updateDate"]
                # при условии, что передает файл, необходимо пересчитать размер
                parent = i['parentId']

                if i['type'] == 'FILE':
                    while 1:
                        if parent is None:
                            break

                        finder = cur.execute(f'SELECT * FROM file WHERE id = "{parent}"').fetchall()

                        if finder[0][6] == 0:
                            cur.execute(f'UPDATE file SET size = {size} WHERE id = "{parent}"')
                        else:
                            size2 = size + finder[0][5]
                            cur.execute(f'UPDATE file SET size = {size2} WHERE id = "{parent}"')

                        cur.execute(f'UPDATE file SET childrenCount = childrenCount+1 WHERE id = "{parent}"')
                        cur.execute(f'UPDATE file SET date = "{dd}" WHERE id = "{parent}"')

                        parent = finder[0][4]
                # Если передается директория, необходимо изменить дату изменения всех родителей директории
                else:
                    parent = i['parentId']

                    while 1:
                        finder = cur.execute(f'SELECT * FROM file WHERE id = "{parent}"').fetchall()
                        if parent is None:
                            break
                        cur.execute(f'UPDATE file SET date = "{dd}" WHERE id = "{parent}"')
                        parent = finder[0][4]
                try:
                    zz = i['url']
                except:
                    zz = None
                # Вставка элементов
                cur.execute('INSERT INTO file VALUES (?,?,?,?,?,?,?)',
                            (i["id"], i["type"], zz, data["updateDate"], i["parentId"], size, childrenCount))

                # Добавляем все элементы во вторую таблицу и удаляем повторяющиеся, для /node/{id}/statistic
                output = cur.execute(f'SELECT * FROM file').fetchall()

                for k in output:
                    cur.execute(f'INSERT INTO file_copy VALUES (?,?,?,?,?,?,?)',
                                 (k[0], k[1], k[2], k[3], k[4], k[5], k[6]))

                cur.execute('DELETE FROM file_copy WHERE EXISTS (SELECT 1 FROM file_copy s2 '
                             'WHERE file_copy.id = s2.id AND file_copy.size = s2.size AND file_copy.childrenCount = '
                             's2.childrenCount AND file_copy.date = s2.date AND file_copy.rowid > s2.rowid)')


            except:
                return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')
        # Если данный id уже встречался
        else:
            return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')

    con.commit()
    return Response("{'message':'Ok'}", status=200, mimetype='application/json')


@app.route('/delete/<id>', methods=['DELETE'])
def delete(id):
    # Проверка валидности введенных данных
    finder = cur.execute(f'SELECT * FROM file WHERE id = "{id}"').fetchall()

    if not finder:
        return Response("{'message':'Item not found'}", status=404, mimetype='application/json')

    parent = finder[0][4]
    size = finder[0][5]

    if size is None:
        size = 0
    children = finder[0][6]

    # Изменение размера и количества детей у родителей
    while 1:
        if parent is None:
            break

        finder = cur.execute(f'SELECT * FROM file WHERE id = "{parent}"').fetchall()
        newsize = finder[0][5]- size
        cur.execute(f'UPDATE file SET size = {newsize} WHERE id = "{parent}"')
        cur.execute(f'UPDATE file SET childrenCount = childrenCount-{children} WHERE id = "{parent}"')
        parent = finder[0][4]

    ids = [id]
    ids1 = [id]

    # Удаление детей
    delete_rows(cur, ids, ids1)
    # Добавление всех изменений в таблицу file_copy для получения статистики в будущем
    for i in ids1:
        cur.execute(f'DELETE FROM file_copy WHERE id = "{i}"')

    output = cur.execute(f'SELECT * FROM file').fetchall()
    for k in output:
        cur.execute(f'INSERT INTO file_copy VALUES (?,?,?,?,?,?,?)',
                     (k[0], k[1], k[2], k[3], k[4], k[5], k[6]))

    cur.execute('DELETE FROM file_copy WHERE EXISTS (SELECT 1 FROM file_copy s2 '
                 'WHERE file_copy.id = s2.id AND file_copy.size = s2.size AND file_copy.childrenCount = '
                 's2.childrenCount AND file_copy.date = s2.date AND file_copy.rowid > s2.rowid)')

    con.commit()
    return Response("{'message':'Ok'}", status=200, mimetype='application/json')


# Функция удаления id и всех его детей
def delete_rows(cur, ids, ids1):
    if not ids:
        return

    finder = cur.execute(f'SELECT * FROM file WHERE parentId = "{ids[0]}"').fetchall()
    for i in finder:
        ids.append(i[0])
        ids1.append(i[0])

    cur.execute(f'DELETE FROM file WHERE id = "{ids[0]}"')
    ids.pop(0)
    delete_rows(cur, ids, ids1)


@app.route('/nodes/<id>', methods=['GET'])
def nodes(id):
    finder = cur.execute(f'SELECT * FROM file WHERE id = "{id}"').fetchall()
    if not finder:
        return Response("{'message':'Item not found'}", status=404, mimetype='application/json')

    result = {}
    print_nodes(id, result, cur)
    result = json.dumps(result)
    con.commit()

    return Response(response=result, status=200, mimetype='application/json')


# Функция вывода элемента и его детей
def print_nodes(id, dictionary, cur):
    finder = cur.execute(f'SELECT * FROM file WHERE id = "{id}"').fetchall()

    dictionary['type'] = finder[0][1]
    dictionary['url'] = finder[0][2]
    dictionary['id'] = finder[0][0]
    dictionary['parentId'] = finder[0][4]
    dictionary['size'] = finder[0][5]
    dictionary['date'] = finder[0][3]

    # Просмотр количества детей
    if finder[0][6] == 0:
        dictionary['children'] = None
        return

    # Если дети есть, то добавляем столько же новых словарей, в которые рекурсивно заходим и пишем информацию о детях
    else:
        dictionary['children'] = []
        finder = cur.execute(f'SELECT * FROM file WHERE parentId = "{id}"').fetchall()
        k = len(finder)
        for j in range(k):
            dictionary['children'].append(dict())
        z = 0
        for i in finder:
            if z == k:
                return
            dictionary['children'][z] == print_nodes(i[0], dictionary['children'][z], cur)
            z += 1


@app.route('/updates', methods=['GET'])
def updates():
    args = request.args['date']
    flag = 1

    try:
        flag = flag * check_time(args)
        if not flag:
            return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')
    except:
        return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')

    # Нахождение всех элементов, которые изменялись за последние 24ч
    finder = cur.execute(
        f'SELECT * FROM file WHERE datetime(date) BETWEEN datetime("{args}", "-1 days") AND datetime("{args}")').fetchall()
    result = []

    # Вывод найденных элементов
    for i in finder:
        if i[1] == 'FILE':
            result.append(
                {'type': i[1], 'url': i[2], 'id': i[0], 'size': i[5], 'parentId': i[4], 'date': i[3]})

    result = json.dumps(result)
    con.commit()
    return Response(response=result, status=200, mimetype='application/json')


@app.route('/node/<id>/history', methods=['GET'])
def history(id):

    start = request.args['dateStart']
    end = request.args['dateEnd']

    finder = cur.execute(f'SELECT * FROM file_copy WHERE id = "{id}"').fetchall()
    if not finder:
        return Response("{'message':'Item not found'}", status=404, mimetype='application/json')

    flag = 1
    try:
        flag = flag * check_time(start) * check_time(end)
        if not flag:
            return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')
    except:
        return Response("{'message':'Validation Failed'}", status=400, mimetype='application/json')

    # Нахождение всех изменений в таблице file_copy в промежутке между двумя заданными датами
    finder = cur.execute(
        f'SELECT * FROM file_copy WHERE id = "{id}" AND datetime(date) BETWEEN datetime("{start}") AND datetime("{end}")')
    result = []
    for i in finder:
        if i[3] == end:
            continue
        if i[5] is None:
            result.append(
                {'type': i[1], 'url': i[2], 'id': i[0], 'size': None, 'parentId': i[4], 'date': i[3]})
        else:
            result.append(
                {'type': i[1], 'url': i[2], 'id': i[0], 'size': i[5], 'parentId': i[4], 'date': i[3]})

    result = json.dumps(result)
    return Response(response=result, status=200, mimetype='application/json')


# Проверка корректности введенного id
def check_id(id):
    id1 = cur.execute(f'SELECT * FROM file WHERE id = "{id}"').fetchall()
    if id1 == []:
        return True
    return False

# Проверка корректности введенного parentId
def check_parentId(id):
    if id is None:
        return True
    match = re.search(r'.{8}-.{4}-.{4}-.{4}-.{12}', id)
    if match is None and id is not None:
        return False

    return True

# Проверка корректности введенного date
def check_time(time):
    match = re.search(r'(\d){4}-(\d){2}-(\d){2}T(\d){2}:(\d){2}:(\d){2}Z', time)
    if match is None:
        return False
    return True

# Проверка корректности введенного size
def check_size(size):
    if size <= 0 or type(size) != int:
        return False
    return True

# Проверка корректности введенного type
def check_type(type):
    if type != 'FILE' and type != 'FOLDER':
        return False
    return True

def check_url(url):
    match = re.search(r'(\/.)+', url)
    if match is None or len(match[0]) > 255:
        return False
    return True

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=80)
