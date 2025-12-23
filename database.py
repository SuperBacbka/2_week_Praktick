import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class Database:
    """
    SQLite БД для учета заявок на ремонт климатического оборудования
    + роль quality_manager
    + help_requests (запросы помощи)
    + дедлайны + продление с согласованием клиента
    + QR-отзывы (в БД не хранится, только ссылка в UI)
    """

    def __init__(self, db_name: str = "service_center.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        self.create_tables()
        self._migrate_schema()
        self.add_default_data()

    # -------------------- SCHEMA --------------------

    def create_tables(self) -> None:
        # users
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                full_name TEXT,
                role TEXT DEFAULT 'specialist',
                phone TEXT,
                email TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # equipment_types
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS equipment_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                description TEXT
            )
        """)

        # requests
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_number TEXT UNIQUE NOT NULL,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                equipment_type TEXT,
                device_model TEXT,
                fault_type TEXT,
                problem_description TEXT,

                customer_name TEXT,
                customer_phone TEXT,

                status TEXT DEFAULT 'открыта',

                assigned_to INTEGER,
                assist_to INTEGER,

                estimated_cost REAL,
                actual_cost REAL,

                deadline TIMESTAMP,
                deadline_extended_to TIMESTAMP,

                extension_reason TEXT,
                client_approval TEXT,
                client_approval_at TIMESTAMP,
                extended_by INTEGER,

                completed_date TIMESTAMP,

                FOREIGN KEY (assigned_to) REFERENCES users(id),
                FOREIGN KEY (assist_to) REFERENCES users(id),
                FOREIGN KEY (extended_by) REFERENCES users(id)
            )
        """)

        # request_comments
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS request_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,

                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                is_ordered_parts BOOLEAN DEFAULT 0,
                parts_description TEXT,

                FOREIGN KEY (request_id) REFERENCES requests(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # status_history
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT,
                changed_by INTEGER,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (request_id) REFERENCES requests(id) ON DELETE CASCADE,
                FOREIGN KEY (changed_by) REFERENCES users(id)
            )
        """)

        # help_requests
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS help_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                requested_by INTEGER NOT NULL,
                message TEXT,
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_by INTEGER,
                resolved_at TIMESTAMP,
                resolution_note TEXT,
                FOREIGN KEY (request_id) REFERENCES requests(id) ON DELETE CASCADE,
                FOREIGN KEY (requested_by) REFERENCES users(id),
                FOREIGN KEY (resolved_by) REFERENCES users(id)
            )
        """)

        self.conn.commit()

    def _migrate_schema(self) -> None:
        """Мягкая миграция схемы: добавит недостающие колонки в существующую БД."""
        # requests columns
        self.cursor.execute("PRAGMA table_info(requests)")
        cols = {row["name"] for row in self.cursor.fetchall()}

        def add_col(col_name: str, ddl: str) -> None:
            if col_name not in cols:
                try:
                    self.cursor.execute(f"ALTER TABLE requests ADD COLUMN {ddl}")
                except Exception as e:
                    print(f"Migration failed: add {col_name}: {e}")

        # Для старых БД (если таблица requests была старой версии)
        add_col("fault_type", "fault_type TEXT")
        add_col("assist_to", "assist_to INTEGER REFERENCES users(id)")
        add_col("deadline", "deadline TIMESTAMP")
        add_col("deadline_extended_to", "deadline_extended_to TIMESTAMP")
        add_col("extension_reason", "extension_reason TEXT")
        add_col("client_approval", "client_approval TEXT")
        add_col("client_approval_at", "client_approval_at TIMESTAMP")
        add_col("extended_by", "extended_by INTEGER REFERENCES users(id)")

        # help_requests already created in create_tables (IF NOT EXISTS)
        self.conn.commit()

    def add_default_data(self) -> None:
        # equipment types (климат)
        equipment_types = [
            ("Кондиционер", "Сплит-системы, мульти-сплит, VRF/VRV"),
            ("Вентиляционная установка", "Приточные/вытяжные установки"),
            ("Тепловая завеса", "Воздушные/тепловые завесы"),
            ("Чиллер", "Охладитель жидкости"),
            ("Фанкойл", "Внутренние блоки фанкойлов"),
            ("Осушитель воздуха", "Промышленные/бытовые осушители"),
            ("Увлажнитель воздуха", "Промышленные/бытовые увлажнители"),
            ("Котельное оборудование", "Автоматика, насосы, датчики"),
        ]
        for et in equipment_types:
            self.cursor.execute(
                "INSERT OR IGNORE INTO equipment_types (name, description) VALUES (?, ?)",
                et
            )

        # admin
        self.cursor.execute("""
            INSERT OR IGNORE INTO users (username, password, full_name, role)
            VALUES (?, ?, ?, ?)
        """, ("admin", _sha256("admin123"), "Администратор системы", "admin"))

        # specialist
        self.cursor.execute("""
            INSERT OR IGNORE INTO users (username, password, full_name, role)
            VALUES (?, ?, ?, ?)
        """, ("ivanov", _sha256("spec123"), "Иванов Иван Иванович", "specialist"))

        # quality manager
        self.cursor.execute("""
            INSERT OR IGNORE INTO users (username, password, full_name, role)
            VALUES (?, ?, ?, ?)
        """, ("qmanager", _sha256("qm123"), "Менеджер по качеству", "quality_manager"))

        self.conn.commit()

    # -------------------- HELPERS --------------------

    @staticmethod
    def _row_to_dict(r: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if r is None:
            return None
        return dict(r)

    @staticmethod
    def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        return [dict(r) for r in rows]

    # -------------------- USERS --------------------

    def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        hp = _sha256(password)
        self.cursor.execute("""
            SELECT * FROM users
            WHERE username = ? AND password = ? AND is_active = 1
        """, (username, hp))
        return self._row_to_dict(self.cursor.fetchone())

    def add_user(self, user_data: Dict[str, Any]) -> Optional[int]:
        try:
            self.cursor.execute("""
                INSERT INTO users (username, password, full_name, role, phone, email, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (
                user_data["username"],
                _sha256(user_data["password"]),
                user_data.get("full_name", ""),
                user_data.get("role", "specialist"),
                user_data.get("phone", ""),
                user_data.get("email", "")
            ))
            self.conn.commit()
            return int(self.cursor.lastrowid)
        except Exception as e:
            print(f"add_user error: {e}")
            return None

    def get_all_users(self, role_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        q = "SELECT * FROM users WHERE is_active = 1"
        params: List[Any] = []
        if role_filter:
            q += " AND role = ?"
            params.append(role_filter)
        q += " ORDER BY full_name"
        self.cursor.execute(q, params)
        return self._rows_to_dicts(self.cursor.fetchall())

    # -------------------- EQUIPMENT --------------------

    def get_equipment_types(self) -> List[Dict[str, Any]]:
        self.cursor.execute("SELECT * FROM equipment_types ORDER BY name")
        return self._rows_to_dicts(self.cursor.fetchall())

    # -------------------- REQUEST NUMBER --------------------

    def generate_request_number(self) -> str:
        prefix = "REQ"
        date_part = datetime.now().strftime("%Y%m%d")
        self.cursor.execute(
            "SELECT COUNT(*) AS cnt FROM requests WHERE request_number LIKE ?",
            (f"{prefix}{date_part}%",)
        )
        cnt = int(self.cursor.fetchone()["cnt"] or 0) + 1
        return f"{prefix}{date_part}{cnt:04d}"

    # -------------------- REQUESTS CRUD --------------------

    def add_request(self, request_data: Dict[str, Any], default_deadline_days: int = 3) -> Optional[int]:
        """Создание заявки + первичная запись истории статусов."""
        try:
            request_number = self.generate_request_number()

            # базовый дедлайн: created_date + N дней (можно менять под ТЗ)
            deadline = (datetime.now() + timedelta(days=default_deadline_days)).strftime("%Y-%m-%d %H:%M:%S")

            self.cursor.execute("""
                INSERT INTO requests (
                    request_number,
                    equipment_type,
                    device_model,
                    fault_type,
                    problem_description,
                    customer_name,
                    customer_phone,
                    status,
                    estimated_cost,
                    deadline
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'открыта', ?, ?)
            """, (
                request_number,
                request_data.get("equipment_type"),
                request_data.get("device_model"),
                request_data.get("fault_type"),
                request_data.get("problem_description"),
                request_data.get("customer_name"),
                request_data.get("customer_phone"),
                float(request_data.get("estimated_cost") or 0),
                deadline
            ))

            request_id = int(self.cursor.lastrowid)

            self.cursor.execute("""
                INSERT INTO status_history (request_id, old_status, new_status, changed_by)
                VALUES (?, ?, ?, ?)
            """, (request_id, None, "открыта", None))

            self.conn.commit()
            return request_id
        except Exception as e:
            print(f"add_request error: {e}")
            return None

    def get_requests(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Список заявок с JOIN на исполнителей.
        """
        q = """
            SELECT
                r.*,
                u1.full_name AS assigned_name,
                u2.full_name AS assist_name,
                u3.full_name AS extended_by_name
            FROM requests r
            LEFT JOIN users u1 ON u1.id = r.assigned_to
            LEFT JOIN users u2 ON u2.id = r.assist_to
            LEFT JOIN users u3 ON u3.id = r.extended_by
            WHERE 1=1
        """
        params: List[Any] = []

        if filters:
            if filters.get("status"):
                q += " AND r.status = ?"
                params.append(filters["status"])
            if filters.get("assigned_to"):
                q += " AND r.assigned_to = ?"
                params.append(int(filters["assigned_to"]))
            if filters.get("search"):
                s = f"%{filters['search']}%"
                q += " AND (r.request_number LIKE ? OR r.customer_name LIKE ? OR r.customer_phone LIKE ?)"
                params.extend([s, s, s])
            if filters.get("date_from"):
                q += " AND DATE(r.created_date) >= ?"
                params.append(filters["date_from"])
            if filters.get("date_to"):
                q += " AND DATE(r.created_date) <= ?"
                params.append(filters["date_to"])

        q += " ORDER BY r.created_date DESC"
        self.cursor.execute(q, params)
        return self._rows_to_dicts(self.cursor.fetchall())

    def get_request(self, request_id: int) -> Optional[Dict[str, Any]]:
        self.cursor.execute("""
            SELECT
                r.*,
                u1.full_name AS assigned_name,
                u2.full_name AS assist_name,
                u3.full_name AS extended_by_name
            FROM requests r
            LEFT JOIN users u1 ON u1.id = r.assigned_to
            LEFT JOIN users u2 ON u2.id = r.assist_to
            LEFT JOIN users u3 ON u3.id = r.extended_by
            WHERE r.id = ?
        """, (int(request_id),))
        return self._row_to_dict(self.cursor.fetchone())

    def update_request(self, request_id: int, update_data: Dict[str, Any]) -> bool:
        """Обновление полей заявки (без истории статусов)."""
        allowed = [
            "equipment_type", "device_model", "fault_type", "problem_description",
            "customer_name", "customer_phone",
            "estimated_cost", "actual_cost",
            "deadline"
        ]
        fields: List[str] = []
        values: List[Any] = []

        for k in allowed:
            if k in update_data:
                fields.append(f"{k}=?")
                values.append(update_data[k])

        if not fields:
            return True

        values.append(int(request_id))
        try:
            self.cursor.execute(
                f"UPDATE requests SET {', '.join(fields)} WHERE id=?",
                values
            )
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"update_request error: {e}")
            return False

    # -------------------- STATUS / ASSIGN --------------------

    def update_request_status(self, request_id: int, new_status: str, changed_by: Optional[int]) -> bool:
        try:
            self.cursor.execute("SELECT status FROM requests WHERE id=?", (int(request_id),))
            row = self.cursor.fetchone()
            if not row:
                return False
            old_status = row["status"]

            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if new_status == "завершена" else None

            # если переводим НЕ в завершена — completed_date обнуляем
            self.cursor.execute("""
                UPDATE requests
                SET status=?, completed_date=?
                WHERE id=?
            """, (new_status, completed_date, int(request_id)))

            self.cursor.execute("""
                INSERT INTO status_history (request_id, old_status, new_status, changed_by)
                VALUES (?, ?, ?, ?)
            """, (int(request_id), old_status, new_status, int(changed_by) if changed_by else None))

            self.conn.commit()
            return True
        except Exception as e:
            print(f"update_request_status error: {e}")
            return False

    def assign_request(self, request_id: int, specialist_id: int) -> bool:
        """Назначить основного мастера."""
        try:
            self.cursor.execute(
                "UPDATE requests SET assigned_to=? WHERE id=?",
                (int(specialist_id), int(request_id))
            )
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"assign_request error: {e}")
            return False

    def set_assistant_master(self, request_id: int, assistant_id: int) -> bool:
        """Подключить дополнительного мастера (assist_to)."""
        try:
            self.cursor.execute(
                "UPDATE requests SET assist_to=? WHERE id=?",
                (int(assistant_id), int(request_id))
            )
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"set_assistant_master error: {e}")
            return False

    def reassign_master(self, request_id: int, new_master_id: int) -> bool:
        """Переназначить основного мастера (assigned_to)."""
        return self.assign_request(request_id, new_master_id)

    # -------------------- DEADLINE EXTEND --------------------

    def extend_deadline(
        self,
        request_id: int,
        new_deadline: str,
        reason: str,
        approval_text: str,
        extended_by: int
    ) -> bool:
        """
        Продление срока по ТЗ:
        - фиксируем новый срок
        - фиксируем причину
        - фиксируем согласование клиента (текст + дата/время)
        - фиксируем кто продлил
        """
        try:
            self.cursor.execute("""
                UPDATE requests
                SET deadline_extended_to=?,
                    extension_reason=?,
                    client_approval=?,
                    client_approval_at=CURRENT_TIMESTAMP,
                    extended_by=?
                WHERE id=?
            """, (new_deadline, reason, approval_text, int(extended_by), int(request_id)))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"extend_deadline error: {e}")
            return False

    # -------------------- HELP REQUESTS --------------------

    def create_help_request(self, request_id: int, requested_by: int, message: str = "") -> Optional[int]:
        try:
            self.cursor.execute("""
                INSERT INTO help_requests (request_id, requested_by, message, status)
                VALUES (?, ?, ?, 'open')
            """, (int(request_id), int(requested_by), message))
            self.conn.commit()
            return int(self.cursor.lastrowid)
        except Exception as e:
            print(f"create_help_request error: {e}")
            return None

    def list_open_help_requests(self) -> List[Dict[str, Any]]:
        try:
            self.cursor.execute("""
                SELECT
                    hr.id AS help_id,
                    hr.request_id,
                    hr.message,
                    hr.created_at,

                    r.request_number,
                    r.status,
                    r.deadline,
                    r.deadline_extended_to,

                    r.assigned_to,
                    u1.full_name AS assigned_name,

                    r.assist_to,
                    u2.full_name AS assist_name,

                    hr.requested_by,
                    u3.full_name AS requested_by_name
                FROM help_requests hr
                JOIN requests r ON r.id = hr.request_id
                LEFT JOIN users u1 ON u1.id = r.assigned_to
                LEFT JOIN users u2 ON u2.id = r.assist_to
                JOIN users u3 ON u3.id = hr.requested_by
                WHERE hr.status='open'
                ORDER BY hr.created_at DESC
            """)
            return self._rows_to_dicts(self.cursor.fetchall())
        except Exception as e:
            print(f"list_open_help_requests error: {e}")
            return []

    def resolve_help_request(self, help_id: int, resolved_by: int, resolution_note: str = "") -> bool:
        try:
            self.cursor.execute("""
                UPDATE help_requests
                SET status='resolved',
                    resolved_by=?,
                    resolved_at=CURRENT_TIMESTAMP,
                    resolution_note=?
                WHERE id=?
            """, (int(resolved_by), resolution_note, int(help_id)))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"resolve_help_request error: {e}")
            return False

    # -------------------- COMMENTS --------------------

    def add_comment(
        self,
        request_id: int,
        user_id: int,
        comment: str,
        is_ordered_parts: bool = False,
        parts_description: str = ""
    ) -> Optional[int]:
        try:
            self.cursor.execute("""
                INSERT INTO request_comments (request_id, user_id, comment, is_ordered_parts, parts_description)
                VALUES (?, ?, ?, ?, ?)
            """, (int(request_id), int(user_id), comment, int(bool(is_ordered_parts)), parts_description))
            self.conn.commit()
            return int(self.cursor.lastrowid)
        except Exception as e:
            print(f"add_comment error: {e}")
            return None

    def get_request_comments(self, request_id: int) -> List[Dict[str, Any]]:
        try:
            self.cursor.execute("""
                SELECT
                    rc.*,
                    u.full_name AS author_name,
                    u.username AS author_username
                FROM request_comments rc
                JOIN users u ON u.id = rc.user_id
                WHERE rc.request_id=?
                ORDER BY rc.created_at DESC
            """, (int(request_id),))
            return self._rows_to_dicts(self.cursor.fetchall())
        except Exception as e:
            print(f"get_request_comments error: {e}")
            return []

    # -------------------- STATUS HISTORY --------------------

    def get_status_history(self, request_id: int) -> List[Dict[str, Any]]:
        try:
            self.cursor.execute("""
                SELECT
                    sh.*,
                    u.full_name AS changed_by_name
                FROM status_history sh
                LEFT JOIN users u ON u.id = sh.changed_by
                WHERE sh.request_id=?
                ORDER BY sh.changed_at DESC
            """, (int(request_id),))
            return self._rows_to_dicts(self.cursor.fetchall())
        except Exception as e:
            print(f"get_status_history error: {e}")
            return []

    # -------------------- STATISTICS --------------------

    def get_statistics(self, period_days: int = 30) -> Dict[str, Any]:
        """
        Статистика за период по created_date:
        - количество заявок
        - завершено/открыто/в работе
        - % выполнения
        - среднее время ремонта
        - распределение по оборудованию
        - распределение по типам неисправностей (fault_type)
        - эффективность специалистов
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=int(period_days))
        start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

        def scalar(sql: str, params: Tuple[Any, ...]) -> int:
            self.cursor.execute(sql, params)
            r = self.cursor.fetchone()
            return int(r[0] or 0)

        total = scalar(
            "SELECT COUNT(*) FROM requests WHERE created_date BETWEEN ? AND ?",
            (start_str, end_str)
        )
        completed = scalar(
            "SELECT COUNT(*) FROM requests WHERE status='завершена' AND created_date BETWEEN ? AND ?",
            (start_str, end_str)
        )
        open_ = scalar(
            "SELECT COUNT(*) FROM requests WHERE status='открыта' AND created_date BETWEEN ? AND ?",
            (start_str, end_str)
        )
        in_progress = scalar(
            "SELECT COUNT(*) FROM requests WHERE status='в процессе ремонта' AND created_date BETWEEN ? AND ?",
            (start_str, end_str)
        )

        self.cursor.execute("""
            SELECT AVG(julianday(completed_date) - julianday(created_date)) AS avg_days
            FROM requests
            WHERE status='завершена'
              AND completed_date IS NOT NULL
              AND created_date BETWEEN ? AND ?
        """, (start_str, end_str))
        avg_days = self.cursor.fetchone()["avg_days"]
        avg_days = round(float(avg_days or 0), 1)

        # equipment stats
        self.cursor.execute("""
            SELECT equipment_type AS name, COUNT(*) AS cnt
            FROM requests
            WHERE created_date BETWEEN ? AND ?
            GROUP BY equipment_type
            ORDER BY cnt DESC
        """, (start_str, end_str))
        equipment_stats = self._rows_to_dicts(self.cursor.fetchall())

        # fault stats
        self.cursor.execute("""
            SELECT COALESCE(NULLIF(TRIM(fault_type), ''), 'Не указано') AS name, COUNT(*) AS cnt
            FROM requests
            WHERE created_date BETWEEN ? AND ?
            GROUP BY COALESCE(NULLIF(TRIM(fault_type), ''), 'Не указано')
            ORDER BY cnt DESC
        """, (start_str, end_str))
        fault_stats = self._rows_to_dicts(self.cursor.fetchall())

        # specialist stats (completed only)
        self.cursor.execute("""
            SELECT
                u.full_name AS specialist,
                COUNT(r.id) AS completed_count,
                AVG(julianday(r.completed_date) - julianday(r.created_date)) AS avg_days
            FROM requests r
            JOIN users u ON u.id = r.assigned_to
            WHERE r.status='завершена'
              AND r.completed_date IS NOT NULL
              AND r.created_date BETWEEN ? AND ?
            GROUP BY r.assigned_to
            ORDER BY completed_count DESC
        """, (start_str, end_str))
        specialist_stats = self._rows_to_dicts(self.cursor.fetchall())
        for s in specialist_stats:
            s["avg_days"] = round(float(s["avg_days"] or 0), 1)

        completion_rate = round((completed / total) * 100, 1) if total else 0.0

        return {
            "period_days": int(period_days),
            "total_requests": total,
            "completed_requests": completed,
            "open_requests": open_,
            "in_progress_requests": in_progress,
            "completion_rate": completion_rate,
            "avg_completion_days": avg_days,
            "equipment_stats": equipment_stats,
            "fault_stats": fault_stats,
            "specialist_stats": specialist_stats
        }

    # -------------------- CLOSE --------------------

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
