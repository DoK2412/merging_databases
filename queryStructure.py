CREATE_TABLE = '''
--создание промежуточной таблици
CREATE TABLE onpass_users_to_onservice_user (
  id SERIAL NOT NULL, 
  partner_id_pss INT, 
  partner_id_bz INT, 
  partner_name_ru VARCHAR(40), 
  airports_id_bz INT, 
  airports_id_pss INT, 
  airport_name_ru VARCHAR(40), 
  points_id_bz INT, 
  points_name_bz VARCHAR(80), 
  points_id_pss INT, 
  point_name_pss VARCHAR(80), 
  PRIMARY KEY (id))
'''


ID_ONPASS_SELECTION = '''
--запрос на выборку id  партнеров из базы бизнеса
SELECT
  p.id,
  p.unique_value, 
  p.id_1c,
  p.token,
  p.login,
  pt.name,
  pt.language_id,
  points.airport_id,
  points.id as "id_points",
  ptr.name as "name_points",
  a.code_iata,
  at.title,
  at.language_id
FROM partners as "p"
INNER JOIN partners_translate pt ON p.id = pt.partner_id
INNER JOIN points ON pt.partner_id = points.partner_id
INNER JOIN point_translates ptr ON points.id = ptr.point_id
INNER JOIN airports a ON points.airport_id = a.id
INNER JOIN airports_translate at ON a.id = at.airport_id
WHERE
  pt.language_id = 1 AND at.language_id = 1 AND ptr.language_code = $1
'''

ADDING_PARTNERS_TO_THE_INTERIM = '''
-- запрос на добавление парнеров в промежуточную таблицу
INSERT INTO 
  onpass_users_to_onservice_user(partner_id_bz, partner_name_ru, airports_id_bz, airport_name_ru, points_id_bz, points_name_bz)
VALUES($1, $2, $3, $4, $5, $6)
'''

CHECKING_THE_PARTNER_IN_THE_DATABASE = '''
-- провека партнера на наличие в базе oss
SELECT
  pt.partner_id,
  pt.name
FROM
  partners_translate AS "pt"
INNER JOIN 
  partners p ON pt.partner_id = p.id
WHERE (p.unique_value = $1 and p.id_1c = $2)
  or (p.unique_value = $1 AND pt.name = $3)
  or (p.id_1c = $2 AND pt.name = $3)
  or (p.token = $4 AND p.unique_value = $1)
  or (p.token = $4 AND p.id_1c = $2)
  or (p.token = $4 AND p.login = $5)
  or (p.token = $4)
'''

ADDING_PARTNER_ID_FROM_PSS = '''
--добавление id партнера из pss в промежуточную таблицу
UPDATE 
  onpass_users_to_onservice_user
SET 
  partner_id_pss=$1
WHERE
  partner_name_ru = $2
'''

CHECKING_THE_AIROPORT_IN_THE_DATABASE = '''
--провека аэропорта на наличие в базе oss
SELECT
  a.id,
  at.title
FROM
  airports AS "a"
INNER JOIN 
  airports_translate at ON a.id = at.airport_id
WHERE 
  at.title = $1 OR a.code_iata = $2
'''

ADDING_AIROPORT_ID_FROM_PSS = '''
--добавление id аэропорта из pss в промежуточную таблицу
UPDATE
  onpass_users_to_onservice_user
SET
  airports_id_pss = $1
WHERE
  airport_name_ru = $2
'''

UPDATED_PARTNER_AND_AIROPORT_FROM_INTERMEDIATE = '''
-- получение обноаденных id для сверки точек
SELECT 
  ou.points_id_bz,
  ou.partner_id_pss,
  ou.airports_id_pss,
  ou.points_name_bz,
  p.id_1c
FROM
  onpass_users_to_onservice_user AS "ou"
INNER JOIN points p ON ou.points_id_bz = p.id
'''

CHECKING_FOR_POINTS_BETWEEN_BASES = '''
-- проверка одинаковых точек в базах с id_1c
SELECT
  p.id,
  pt.title
FROM
  points AS "p"
INNER JOIN point_translates pt ON p.id = pt.point_id
WHERE
  (p.airport_id = $1 AND p.partner_id = $2 AND p.id_1c = $4 AND pt.language_code = $5) OR
  (p.airport_id = $1 AND p.partner_id = $2 AND pt.title = $3 AND pt.language_code = $5)
'''

SIMILAR_POINTS = '''
-- запрос на добавление схожих точек в промежуточную базу
UPDATE
  onpass_users_to_onservice_user
SET
  points_id_pss = $1,
  point_name_pss = $2
WHERE
  points_id_bz = $3
'''

CHECKING_FOR_POINTS_BETWEEN = '''
-- проверка одинаковых точек в базах  без id_1c
SELECT
  p.id,
  pt.title
FROM
  points AS "p"
INNER JOIN point_translates pt ON p.id = pt.point_id
WHERE
  p.airport_id = $1 AND p.partner_id = $2 AND pt.title = $3 AND pt.language_code = $4
'''

SAMPLING_OF_MISSING_POINTS = '''
--выборка точек которые отсутствуют в pss
SELECT 
  *
FROM
  onpass_users_to_onservice_user as ou
WHERE
  ou.points_id_pss is NULL
'''

GETTING_CITY_DATA = '''
--получение данных о городе для переноса
SELECT 
  airports.id,
  airports.code_iata,
  airports.parent_id,
  airports.latitude AS "al",
  airports.longitude AS "alo",
  airports.created_date,
  airports.photo_path,
  airports.active,
  airports.city_id,
  airports.terminal,
  airports.auid,
  cities.id,
  cities.latitude AS "cl",
  cities.longitude AS "clo",
  cities.created_date,
  cities.timezone,
  cities.cuid,
  ct.city_id,
  ct.title,
  ct.language_id
  
FROM
  airports
INNER JOIN cities ON airports.city_id = cities.id
INNER JOIN cities_translate ct ON cities.id = ct.city_id
WHERE
  airports.id = $1
'''

CHECKING_THE_CITY_FOR_AVAILABILITY = '''
--проверка наличия города в базе pss
SELECT city_id FROM cities_translate WHERE title = $1
'''

ADDING_A_CITY = '''
--добавление  названия города в pss
INSERT INTO 
  cities_translate(city_id, title, language_id)
VALUES($1, $2, $3)
'''

ADDING_AN_TOWN = '''
-- добавление нового города в pss
INSERT INTO 
  cities(latitude, longitude, created_date, timezone, cuid)
VALUES($1, $2, $3, $4, $5)
'''

REQUEST_FOR_A_NEW_CITY_ID = '''
--Ззапрос йд нового города
SELECT id FROM cities WHERE cuid = $1
  '''

ADDING_A_NEW_AIRPORT = '''
--добавление нового аэропорта в pss
INSERT INTO 
  airports(code_iata, parent_id, latitude, longitude, created_date, photo_path, active, city_id, terminal, auid)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
'''

GETTING_PARTNER_DATA = '''
--выборка партнеров которых нет в pss
SELECT
  *
FROM
  partners
INNER JOIN partners_translate pt ON partners.id = pt.partner_id
WHERE 
  partners.id = $1
'''

THE_PARTNER_IN_THE_DATABASE = '''
--добавление партнера в pss
INSERT INTO 
  partners(unique_value, logo_path, testing, active, created_date, photo_path, relevant, relevant_order, login, password, id_1c, cashback_part, token)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
'''

GETTING_A_NEW_ID_PARTNER = '''
--получение нового йд партнера из базы pss
SELECT id FROM partners WHERE unique_value = $1
'''

ADDING_SECONDARY_PARTNER_DATA = '''
--добавление второстепенныз данных о партнере
INSERT INTO 
  partners_translate(partner_id, name, description_short, description, language_id)
VALUES($1, $2, $3, $4, $5)
'''


REQUESTING_DATA_FROM_A_POINT = '''
--запрос данных точки с переносимой базы
SELECT * FROM points WHERE id = $1
'''

TRANSFERRING_POINTS = '''
--запрос на добавление партнера в pss
INSERT INTO 
  points(airport_id, partner_id, created_date, active, close_date, open_date, floor, visible, clear)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
'''

REQUEST_FOR_A_NEW_AIROPORT_ID = '''
--запрос нового йд аэропорта
SELECT id FROM airports WHERE auid = $1
'''
