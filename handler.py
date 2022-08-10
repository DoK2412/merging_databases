import asyncpg
import settings


from queryStructure import *

async def copying_database(request):
  """
  Функция переноса данных в промежуточную таблицу
  :param request:
  :return:
  """
  pool_p = await asyncpg.create_pool(database=settings.DB_NAME_P,
                                    user=settings.DB_USER,
                                    password=settings.DB_PASSWORD,
                                    host=settings.DB_HOST)

  pool_b = await asyncpg.create_pool(database=settings.DB_NAME_B,
                               user=settings.DB_USER,
                               password=settings.DB_PASSWORD,
                               host=settings.DB_HOST)
  #

  async with pool_p.acquire() as con_p:
    #  создание промежуточной таблици
    # await con_p.execute(CREATE_TABLE)
    # выборка данных из таблицы парнеров бизнеса
    partner = await con_p.fetch(ID_ONPASS_SELECTION, 'ru')
    # добавление парнеров в промежуточную таблицу
    for i_partner in partner:
      await con_p.execute(ADDING_PARTNERS_TO_THE_INTERIM, i_partner['id'],
                           i_partner['name'],
                           i_partner['airport_id'],
                           i_partner['title'],
                           i_partner['id_points'],
                           i_partner['name_points']
                           )

  async with pool_b.acquire() as con_b:
    for i_partner in partner:
      # провека партнера на наличие в базе pss
      partners = await con_b.fetch(CHECKING_THE_PARTNER_IN_THE_DATABASE,
                                    i_partner['unique_value'],
                                    i_partner['id_1c'],
                                    i_partner['name'],
                                    i_partner['token'],
                                    i_partner['login'])
      # провека аэропорта на наличие в базе pss
      airoport = await con_b.fetch(CHECKING_THE_AIROPORT_IN_THE_DATABASE,
                                    i_partner['title'],
                                    i_partner['code_iata'])

      if partners:
        async with pool_p.acquire() as con_p:
          # добавление id партнера из pss в промежуточную таблицу
          await con_p.execute(ADDING_PARTNER_ID_FROM_PSS, partners[0]['partner_id'],
                               i_partner['name'] )
      if airoport:
        async with pool_p.acquire() as con_p:
          # провека аэропорта на наличие в базе pss
          await con_p.execute(ADDING_AIROPORT_ID_FROM_PSS, airoport[0]['id'],
                               i_partner['title'] )

  async with pool_p.acquire() as con_p:
    # получение обноаденных id для сверки точек
    preliminary_selection = await con_p.fetch(UPDATED_PARTNER_AND_AIROPORT_FROM_INTERMEDIATE)

  async with pool_b.acquire() as con_b:
    for i_sel in preliminary_selection:
      # проверка одинаковых точек в базах с id_1c
      similar = await con_b.fetch(CHECKING_FOR_POINTS_BETWEEN_BASES,
                                   i_sel['airports_id_pss'],
                                   i_sel['partner_id_pss'],
                                   i_sel['points_name_bz'],
                                   i_sel['id_1c'],
                                   'ru')
      if similar:
        async with pool_p.acquire() as con_p:
          # запрос на добавление схожих точек в промежуточную базу
          await con_p.execute(SIMILAR_POINTS,
                               similar[0]['id'],
                               similar[0]['title'],
                               i_sel['points_id_bz'])




  async with pool_p.acquire() as con_p:
    not_point = await con_p.fetch(SAMPLING_OF_MISSING_POINTS)

  for i_point in not_point:
    id_airoport = i_point['airports_id_pss']
    id_partner = i_point['partner_id_pss']
    if i_point['partner_id_pss'] is None:

      async with pool_p.acquire() as con_p:
        partner = await con_p.fetch(GETTING_PARTNER_DATA,
                                 i_point['partner_id_bz'],
                                    )
        async with pool_b.acquire() as con_b:
          partner_re = await con_b.fetch(GETTING_A_NEW_ID_PARTNER,
                                         partner[0]['unique_value'],
                                         )
          if partner_re:
            id_partner = partner_re[0]['id']
          if len(partner_re) == 0:
            await con_b.execute(THE_PARTNER_IN_THE_DATABASE,
                                partner[0]['unique_value'],
                                partner[0]['logo_path'],
                                partner[0]['testing'],
                                partner[0]['active'],
                                partner[0]['created_date'],
                                partner[0]['photo_path'],
                                partner[0]['relevant'],
                                partner[0]['relevant_order'],
                                partner[0]['login'],
                                partner[0]['password'],
                                partner[0]['id_1c'],
                                partner[0]['cashback_part'],
                                partner[0]['token'])

            new_partner_id = await con_b.fetch(GETTING_A_NEW_ID_PARTNER,
                                               partner[0]['unique_value'])
            id_partner = new_partner_id[0]['id']
            await con_b.execute(ADDING_SECONDARY_PARTNER_DATA,
                                new_partner_id[0]['id'],
                                partner[0]['name'],
                                partner[0]['description_short'],
                                partner[0]['description'],
                                partner[0]['language_id']
                                )

    if i_point['airports_id_pss'] is None:

      async with pool_p.acquire() as con_p:
        city = await con_p.fetch(GETTING_CITY_DATA,
                                 i_point['airports_id_bz'])
        # добавление аэропортов в pss
        async with pool_b.acquire() as con_b:
          # проверка наличия города в базе pss
          new_city = await con_b.fetch(CHECKING_THE_CITY_FOR_AVAILABILITY,
                                       city[0]['title'])

          if len(new_city) == 0:
            # добавление нового города в pss
            await con_b.execute(ADDING_AN_TOWN,
                                city[0]['cl'],
                                city[0]['clo'],
                                city[0]['created_date'],
                                city[0]['timezone'],
                                city[0]['cuid']
                                )
            # Ззапрос йд нового города
            new_city_id = await con_b.fetch(REQUEST_FOR_A_NEW_CITY_ID,
                                            city[0]['cuid'])
            # добавление  названия города в pss
            for i_city in city:
              async with pool_b.acquire() as con_b:
                await con_b.execute(ADDING_A_CITY,
                                    new_city_id[0]['id'],
                                    i_city['title'],
                                    i_city['language_id'])
            # добавление нового аэропорта в pss
            async with pool_b.acquire() as con_b:

              await con_b.execute(ADDING_A_NEW_AIRPORT,
                                  city[0]['code_iata'],
                                  city[0]['parent_id'],
                                  city[0]['al'],
                                  city[0]['alo'],
                                  city[0]['created_date'],
                                  city[0]['photo_path'],
                                  city[0]['active'],
                                  new_city_id[0]['id'],
                                  city[0]['terminal'],
                                  city[0]['auid'])
              id_airoports = await con_b.fetch(REQUEST_FOR_A_NEW_AIROPORT_ID,
                                               city[0]['auid'])

              id_airoport = id_airoports[0]['id']


    async with pool_p.acquire() as con_p:
      point = await con_p.fetch(REQUESTING_DATA_FROM_A_POINT, i_point['points_id_bz'])
    async with pool_b.acquire() as con_b:
      await con_b.execute(TRANSFERRING_POINTS,
                          id_airoport,
                          id_partner,
                          point[0]['created_date'],
                          point[0]['active'],
                          point[0]['close_date'],
                          point[0]['open_date'],
                          point[0]['floor'],
                          point[0]['visible'],
                          point[0]['clear'])
