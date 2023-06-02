from src.gpw.database.postgresql_connection import postgres_connection


def test_postgresql_gpw():
    """
    GIVEN
    WHEN
    THEN
    """
    gpw = postgres_connection()
    result = gpw.get_establecimiento_codigo(16101001)
    print(result)
