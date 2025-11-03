import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import pyodbc
import os
from dotenv import load_dotenv
load_dotenv()


class MSQLServer:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def create_connection(self):
        server = os.getenv('MSQL_SERVER')
        database = os.getenv('MSQL_DATABASE')
        username = os.getenv('MSQL_USERNAME')
        password = os.getenv('MSQL_PASSWORD')
        driver = os.getenv('MSQL_DRIVER')
        
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        self.conn = pyodbc.connect(conn_str)
        return self.conn
    
    def __enter__(self):
        self.create_connection()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _safe_get(self, d, *keys, default=None):
        """Lấy giá trị an toàn từ JSON lồng nhau"""
        for k in keys:
            if isinstance(d, dict):
                d = d.get(k, default)
            else:
                return default
        return d

    def insert_dim_actor(self, actor):
        account_name = self._safe_get(actor, "account", "name")
        account_homepage = self._safe_get(actor, "account", "homePage")
        actor_name = actor.get("name")
        object_type = actor.get("objectType")

        if not account_name:
            return

        self.cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_actor_account WHERE actor_account_name = ?)
            INSERT INTO dim_actor_account (actor_account_name, actor_account_homePage, actor_name, actor_objectType)
            VALUES (?, ?, ?, ?)
        """, account_name, account_name, account_homepage, actor_name, object_type)


    def insert_dim_verb(self, verb):
        verb_id = verb.get("id")
        verb_display = self._safe_get(verb, "display", "en")
        if not verb_id:
            return
        self.cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_verb WHERE verb_id = ?)
            INSERT INTO dim_verb (verb_id, verb_display) VALUES (?, ?)
        """, verb_id, verb_id, verb_display)


    def insert_activity_detail(self, activity):
        activity_id = activity.get("id")
        if not activity_id:
            return

        name = self._safe_get(activity, "definition", "name", "en")
        type_ = self._safe_get(activity, "definition", "type")
        obj_type = activity.get("objectType")

        self.cursor.execute("""
            SELECT definition_name, definition_type, objectType
            FROM activity_detail
            WHERE activity_id = ?
        """, (activity_id,))
        row = self.cursor.fetchone()

        if not row:
            self.cursor.execute("""
                INSERT INTO activity_detail (activity_id, definition_name, definition_type, objectType)
                VALUES (?, ?, ?, ?)
            """, (activity_id, name, type_, obj_type))
        else:
            update_needed = False
            if (not row.definition_name or row.definition_name.strip() == "") and name:
                update_needed = True
            if (not row.definition_type or row.definition_type.strip() == "") and type_:
                update_needed = True
            if (not row.objectType or row.objectType.strip() == "") and obj_type:
                update_needed = True

            if update_needed:
                self.cursor.execute("""
                    UPDATE activity_detail
                    SET definition_name = COALESCE(NULLIF(?, ''), definition_name),
                        definition_type = COALESCE(NULLIF(?, ''), definition_type),
                        objectType = COALESCE(NULLIF(?, ''), objectType)
                    WHERE activity_id = ?
                """, (name, type_, obj_type, activity_id))


    def insert_dim_context(self, context):
        context_registration = context.get("registration")
        context_language = context.get("language")
        context_platform = context.get("platform")

        ext_info = self._safe_get(context, "extensions", "http://lrs.learninglocker.net/define/extensions/info", default={})
        moodle_version = ext_info.get("http://moodle.org")
        plugin_repo = ext_info.get("https://github.com/xAPI-vle/moodle-logstore_xapi")
        event_name = ext_info.get("event_name")
        event_function = ext_info.get("event_function")

        self.cursor.execute("""
            SET NOCOUNT ON;
            DECLARE @newid UNIQUEIDENTIFIER = NEWID();
            INSERT INTO dim_context (
                context_id, context_registration, context_language, context_platform,
                moodle_version, plugin_repo, event_name, event_function
            )
            VALUES (@newid, ?, ?, ?, ?, ?, ?, ?);
            SET NOCOUNT OFF;
            SELECT @newid AS context_id;
        """, context_registration, context_language, context_platform,
             moodle_version, plugin_repo, event_name, event_function)

        row = self.cursor.fetchone()
        return row.context_id if row else None


    def insert_bridge_context_activity(self, context_id, context_activities):
        if not context_activities:
            return

        for role, acts in context_activities.items():
            if isinstance(acts, list):
                for act in acts:
                    activity_id = act.get("id")
                    if not activity_id:
                        continue
                    self.insert_activity_detail(act) # Use self.insert_activity_detail
                    self.cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM bridge_context_activity
                            WHERE context_id = ? AND activity_id = ? AND role = ?
                        )
                        INSERT INTO bridge_context_activity (context_id, activity_id, role)
                        VALUES (?, ?, ?)
                    """, context_id, activity_id, role, context_id, activity_id, role)


    def insert_fact_statement(self, stmt, context_id):
        statement_id = stmt.get("id")
        actor_name = self._safe_get(stmt, "actor", "account", "name")
        verb_id = self._safe_get(stmt, "verb", "id")
        object_id = self._safe_get(stmt, "object", "id")
        timestamp = stmt.get("timestamp")
        stored = stmt.get("stored")
        version = stmt.get("version")
        authority_name = self._safe_get(stmt, "authority", "account", "name")
        authority_home = self._safe_get(stmt, "authority", "account", "homePage")

        if not (statement_id and actor_name and verb_id and object_id):
            return

        self.cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM fact_statement WHERE statement_id = ?)
            INSERT INTO fact_statement (
                statement_id, actor_account_name, verb_id, object_activity_id,
                context_id, timestamp, stored, version,
                authority_account_name, authority_account_homePage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, statement_id, statement_id, actor_name, verb_id, object_id, context_id,
             timestamp, stored, version, authority_name, authority_home)

# def get_activity_chain(registration_id):
#     conn = connect_db()
#     cursor = conn.cursor()

#     # Lấy context_id tương ứng
#     cursor.execute("""
#         SELECT context_id
#         FROM dim_context
#         WHERE context_registration = ?
#     """, registration_id)
#     row = cursor.fetchone()
#     if not row:
#         print(f"❌ Không tìm thấy context với registration: {registration_id}")
#         return
#     context_id = row.context_id

#     # Lấy danh sách activity theo context_id
#     cursor.execute("""
#         SELECT b.role, a.activity_id, a.definition_name, a.definition_type, a.objectType
#         FROM bridge_context_activity b
#         JOIN activity_detail a ON b.activity_id = a.activity_id
#         WHERE b.context_id = ?
#         ORDER BY 
#             CASE b.role 
#                 WHEN 'category' THEN 1
#                 WHEN 'parent' THEN 2
#                 WHEN 'other' THEN 3
#                 ELSE 4
#             END
#     """, context_id)

#     activities = cursor.fetchall()

#     # Gom nhóm theo role
#     result = {}
#     for row in activities:
#         role = row.role
#         if role not in result:
#             result[role] = []
#         result[role].append({
#             "activity_id": row.activity_id,
#             "name": row.definition_name,
#             "type": row.definition_type,
#             "objectType": row.objectType
#         })

#     print(f"✅ Context ID: {context_id}")
#     print(json.dumps(result, indent=4, ensure_ascii=False))

#     cursor.close()
#     conn.close()


<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
class MSQLServer:
    def __init__(self):
        self.conn = self.create_connection()

    def create_connection(self):
        server = os.getenv('MSQL_SERVER')
        database = os.getenv('MSQL_DATABASE')
        username = os.getenv('MSQL_USERNAME')
        password = os.getenv('MSQL_PASSWORD')
        driver = os.getenv('MSQL_DRIVER')  # dùng driver mặc định (không cần bản 17)
        
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"
        conn = pyodbc.connect(conn_str)
        return conn
    
    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
