import asyncpg

import settings
from aiohttp import web


from handler import copying_database

async def on_start(app):
    """
    функция подключения к базе данных при входе
    :param app:
    :return:
    """
    app['db_bisnes'] = await asyncpg.connect(**settings.pool_settings_p)
    app['db_bancs'] = await asyncpg.connect(**settings.pool_settings_b)


async def on_closed(app):
    """
    функция отключения базу данных при выходе
    :param app:
    :return:
    """
    await app['db_bisnes'].close()
    await app['db_bancs'].close()


async def make_app():

    app = web.Application()
    app.on_startup.append(on_start)
    app.add_routes([
        web.get('/copying_database', copying_database)
    ])
    app.on_cleanup.append(on_closed)

    return app


if __name__ == '__main__':
    web.run_app(make_app())