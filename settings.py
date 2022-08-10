DB_HOST = ''
DB_PORT = ''
DB_NAME_P = ''
DB_USER = 'dok2414'
DB_PASSWORD = ''

DB_NAME_B = 'bank_space'

pool_settings_p = dict(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME_P,
    command_timeout=30
)

pool_settings_b = dict(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME_B,
    command_timeout=30
)