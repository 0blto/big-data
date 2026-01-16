from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


class PostgresInterface(ABC):
    @abstractmethod
    def run(self, sql: str, parameters: Optional[tuple] = None) -> Any: ...
    @abstractmethod
    def get_first(self, sql: str, parameters: Optional[tuple] = None) -> Any: ...
    @abstractmethod
    def get_all(self, sql: str, parameters: Optional[tuple] = None) -> List[Dict[str, Any]]: ...
    @abstractmethod
    def insert(self, table: str, data: Dict[str, Any]) -> None: ...
    @abstractmethod
    def insert_ignore(self, table: str, data: Dict[str, Any]) -> None: ...
    @abstractmethod
    def select(self, table: str, columns: Optional[List[str]] = None,
               where: Optional[str] = None, params: Optional[Tuple] = None) -> List[Dict[str, Any]]: ...
    @abstractmethod
    def upsert(self, table: str, data: Dict[str, Any], conflict_column: str) -> None: ...
    @abstractmethod
    def insert_versioned(
        self,
        table: str,
        business_key_column: str,
        business_key_value: Any,
        data: Dict[str, Any],
        sk_column: str = "id"
    ) -> bool: ...


class PostgresHookWrapper(PostgresInterface):
    def __init__(self, conn_id: str = "ods_postgres"):
        self.hook = PostgresHook(postgres_conn_id=conn_id)

    def run(self, sql: str, parameters: Optional[tuple] = None) -> Any:
        return self.hook.run(sql, parameters=parameters)

    def get_first(self, sql: str, parameters: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        conn = self.hook.get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, parameters)
            row = cur.fetchone()
            if not row:
                return None
            col_names = [desc[0] for desc in cur.description]
            return dict(zip(col_names, row))

    def get_all(self, sql: str, parameters: Optional[tuple] = None) -> List[Dict[str, Any]]:
        df = self.hook.get_pandas_df(sql, parameters=parameters)
        return df.to_dict(orient="records")

    def insert(self, table: str, data: Dict[str, Any]) -> None:
        keys = ', '.join(data.keys())
        vals = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO {table} ({keys}) VALUES ({vals})"
        self.hook.run(sql, parameters=tuple(data.values()))

    def insert_ignore(self, table: str, data: Dict[str, Any]) -> None:
        keys = ', '.join(data.keys())
        vals = ', '.join(['%s'] * len(data))
        sql = f"""
            INSERT INTO {table} ({keys})
            VALUES ({vals})
            ON CONFLICT DO NOTHING
        """
        self.hook.run(sql, parameters=tuple(data.values()))

    def select(self, table: str, columns: Optional[List[str]] = None,
               where: Optional[str] = None, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        cols = ', '.join(columns) if columns else '*'
        sql = f"SELECT {cols} FROM {table}"
        if where:
            sql += f" WHERE {where}"

        conn = self.hook.get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]

        result = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            for k, v in row_dict.items():
                if isinstance(v, datetime):
                    row_dict[k] = v.isoformat()
            result.append(row_dict)

        return result

    def upsert(self, table: str, data: Dict[str, Any], conflict_column: str) -> None:
        keys = ', '.join(data.keys())
        vals = ', '.join(['%s'] * len(data))
        update = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys() if k != conflict_column])
        sql = f"""
            INSERT INTO {table} ({keys}) VALUES ({vals})
            ON CONFLICT ({conflict_column}) DO UPDATE SET {update if update else conflict_column} = {conflict_column}
        """
        self.hook.run(sql, parameters=tuple(data.values()))

    def insert_versioned(
            self,
            table: str,
            key_column: str,
            key_value: Any,
            data: Dict[str, Any],
            unique_column: Optional[str] = None
    ) -> bool:
        now = datetime.now()

        if unique_column:
            row = self.get_first(f"SELECT * FROM {table} WHERE {unique_column}=%s AND is_current=true",
                                 (data[unique_column],))
            if row:
                return False

        rows = self.select(
            table,
            where=f"{key_column} = %s AND is_current = true",
            params=(key_value,)
        )
        current = rows[0] if rows else None

        if not current:
            data_full = {
                **data,
                key_column: key_value,
                "valid_from": now,
                "valid_to": datetime(9999, 12, 31),
                "is_current": True,
                "version": 1
            }
            self.insert(table, data_full)
            return True

        service_fields = {"valid_from", "valid_to", "is_current", "version"}
        new_data = {k: v for k, v in data.items() if k not in service_fields}

        common_keys = (set(current.keys()) | set(data.keys())) - service_fields

        old_data_common = {k: current.get(k) for k in common_keys}
        new_data_common = {k: data.get(k, current.get(k)) for k in common_keys}

        if old_data_common == new_data_common:
            return False
        print(old_data_common)
        print(new_data_common)

        self.run(
            f"UPDATE {table} SET valid_to=%s, is_current=false WHERE {key_column}=%s AND is_current=true",
            (now, key_value)
        )

        data_full = {
            **new_data,
            "valid_from": now,
            "valid_to": datetime(9999, 12, 31),
            "is_current": True,
            "version": current["version"] + 1
        }
        self.insert(table, data_full)
        return True


def get_postgres_client() -> PostgresInterface:
    return PostgresHookWrapper()