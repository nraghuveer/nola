import asyncpg
from typing import List, Dict
import asyncio

# TODO: execute queries with timeout


class SchemaCachePg:
    """ Cache for the schema of the database """

    def __init__(
        self, connection_pool: asyncpg.pool.Pool, loop: asyncio.AbstractEventLoop
    ):
        """

        :param connection_pool: Connection pool for the database
        :type connection_pool: asyncpg.pool.Pool
        """
        self._pool = connection_pool
        self._username = "postgres"  # TODO: remove this hardcoded
        self._schemas = []
        self._tables = {}

    async def refresh(self) -> None:
        """Refresh the scheam cache
        """
        self._schemas = self._fetch_schemas()
        self._tables = self._fetch_tables(self._schemas)

    async def _get_connection(self,) -> asyncpg.connection.Connection:
        """Returns a connection from the pool

        :return: connection
        :rtype: asyncpg.connection.Connection
        """
        await self._pool.acquire()

    async def _fetch_schemas(self) -> List[str]:
        """Returns list of schemas for given connection

        :return: list of schemes for given connection
        :rtype: List[str]
        """
        # query = "select schema_name as name from information_schema.schemata"
        result = ["production"]
        return [schema for schema in result if not schema.startswith("pg_")]

    async def _fetch_tables(self, schemas: List[str]) -> Dict[str, List[str]]:
        """Returns tables for each of the given schema

        :param schemas: list of schemas to fetch tables for
        :type schemas: List[str]
        :return: Dictionary with schema name as key and list of tables\
            for that respective schema as value
        :rtype: Dict[str, List[str]]
        """
        query = (
            "select table_name for information_schema.tables "
            "where table_schema in ({schemas});"
        ).format(schemas=",".join(f"'{x}'" for x in schemas))
        print(query)
        async with self._get_connection() as conn:
            return [row.table_name for row in list(await conn.fetch(query))]


class DB:
    pass


class View:
    pass


class Frame:
    pass
