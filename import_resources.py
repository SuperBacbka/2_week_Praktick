import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import hashlib
from pathlib import Path

# Пути к CSV (как у вас загружено)
CSV_USERS = Path("inputDataUsers.csv")
CSV_REQUESTS = Path("inputDataRequests.csv")
CSV_COMMENTS = Path("inputDataComments.csv")

# Куда импортировать
DB_PATH = Path("service_center.db")

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def parse_date(s: str):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    if not s:
        return None
    # В ваших данных даты вида 2023-06-06 / 2023-08-03
    # Сделаем время 00:00:00
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # запасной вариант
        return s

def map_role(rus_type: str) -> str:
    # В inputDataUsers.csv типы: "Мастер", "Менеджер", "Оператор"
    # Маппим под роли приложения:
    t = (rus_type or "").strip()
    if t == "Мастер":
        return "specialist"
    if t == "Менеджер":
        # если в ваших ресурсах "Менеджер" = менеджер качества — оставляем так
        # иначе можно поменять на "manager"
        return "quality_manager"
    if t == "Оператор":
        # роли "operator" в UI нет, но хранить можно (просто будет минимум пунктов меню)
        return "operator"
    return "specialist"

def map_status(rus_status: str) -> str:
    s = (rus_status or "").strip()
    if s == "Новая заявка":
        return "открыта"
    if s == "В процессе ремонта":
        return "в процессе ремонта"
    if s == "Готова к выдаче":
        # в вашем приложении ближайший аналог — завершена
        return "завершена"
    # на всякий случай
    return "открыта"

def main():
    # Подключаемся к БД (она должна быть создана вашим database.py заранее)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Проверим, что нужные таблицы есть
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    required = {"users", "requests", "request_comments", "status_history", "equipment_types", "help_requests"}
    missing = required - tables
    if missing:
        raise RuntimeError(
            f"В БД {DB_PATH} нет таблиц {missing}. "
            f"Сначала создайте БД вашим Database() (запустите приложение или отдельный init)."
        )

    users = pd.read_csv(CSV_USERS, sep=";", engine="python")
    reqs = pd.read_csv(CSV_REQUESTS, sep=";", engine="python")
    comm = pd.read_csv(CSV_COMMENTS, sep=";", engine="python")

    # 1) Импорт users: строим соответствие внешний userID -> внутренний users.id
    user_map = {}  # ext_user_id -> internal_id
    print("Импорт пользователей...")

    for _, row in users.iterrows():
        ext_id = int(row["userID"])
        fio = str(row.get("fio", "") or "").strip()
        phone = str(row.get("phone", "") or "").strip()
        login = str(row.get("login", "") or "").strip()
        password = str(row.get("password", "") or "").strip()
        rtype = str(row.get("type", "") or "").strip()

        if not login:
            continue

        role = map_role(rtype)
        hp = sha256(password if password else "123456")

        cur.execute("""
            INSERT OR IGNORE INTO users (username, password, full_name, role, phone, email, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (login, hp, fio, role, phone, ""))

        # Получаем id (если уже был — достанем)
        cur.execute("SELECT id FROM users WHERE username=?", (login,))
        internal_id = int(cur.fetchone()[0])
        user_map[ext_id] = internal_id

    conn.commit()
    print(f"Пользователей импортировано: {len(user_map)}")

    # Вспомогательно: клиентские данные (fio/phone) по clientID
    client_info = {}
    for _, row in users.iterrows():
        ext_id = int(row["userID"])
        client_info[ext_id] = {
            "fio": str(row.get("fio", "") or "").strip(),
            "phone": str(row.get("phone", "") or "").strip(),
        }

    # 2) Импорт requests
    print("Импорт заявок...")
    request_map = {}  # ext requestID -> internal requests.id

    for _, row in reqs.iterrows():
        ext_req_id = int(row["requestID"])
        start_date = parse_date(row.get("startDate"))
        eq_type = str(row.get("homeTechType", "") or "").strip()
        model = str(row.get("homeTechModel", "") or "").strip()
        problem = str(row.get("problemDescryption", "") or "").strip()
        rus_status = str(row.get("requestStatus", "") or "").strip()
        status = map_status(rus_status)

        completion = parse_date(row.get("completionDate"))
        master_id_ext = row.get("masterID")
        client_id_ext = row.get("clientID")

        master_internal = None
        if master_id_ext is not None and not (isinstance(master_id_ext, float) and pd.isna(master_id_ext)):
            master_internal = user_map.get(int(master_id_ext))

        cust_name = ""
        cust_phone = ""
        if client_id_ext is not None and not (isinstance(client_id_ext, float) and pd.isna(client_id_ext)):
            ci = client_info.get(int(client_id_ext), {})
            cust_name = ci.get("fio", "")
            cust_phone = ci.get("phone", "")

        # Сформируем номер заявки из requestID (чтобы было стабильно)
        request_number = f"IMP{ext_req_id:05d}"

        # Плановый срок: +3 дня от даты старта (если нет — от now)
        try:
            base_dt = datetime.strptime(start_date[:10], "%Y-%m-%d") if start_date else datetime.now()
        except Exception:
            base_dt = datetime.now()
        deadline = (base_dt + timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")

        # Вставка заявки. created_date задаём явно.
        cur.execute("""
            INSERT INTO requests (
                request_number, created_date,
                equipment_type, device_model, fault_type, problem_description,
                customer_name, customer_phone,
                status, assigned_to,
                estimated_cost, actual_cost,
                deadline, completed_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request_number, start_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            eq_type, model, "", problem,
            cust_name, cust_phone,
            status, master_internal,
            0.0, None,
            deadline,
            completion if status == "завершена" else None
        ))

        internal_req_id = int(cur.lastrowid)
        request_map[ext_req_id] = internal_req_id

        # История статусов: первая запись
        cur.execute("""
            INSERT INTO status_history (request_id, old_status, new_status, changed_by)
            VALUES (?, ?, ?, ?)
        """, (internal_req_id, None, status, master_internal))

        # repairParts -> комментарий "заказаны комплектующие"
        repair_parts = row.get("repairParts")
        if repair_parts is not None and not (isinstance(repair_parts, float) and pd.isna(repair_parts)):
            parts_text = str(repair_parts).strip()
            if parts_text:
                # автором сделаем мастера, если есть, иначе admin (id=1 не гарантирован — поэтому None допустим)
                author = master_internal or list(user_map.values())[0]
                cur.execute("""
                    INSERT INTO request_comments
                    (request_id, user_id, comment, is_ordered_parts, parts_description)
                    VALUES (?, ?, ?, ?, ?)
                """, (internal_req_id, author, "Заказаны комплектующие", 1, parts_text))

    conn.commit()
    print(f"Заявок импортировано: {len(request_map)}")

    # 3) Импорт comments
    print("Импорт комментариев...")
    imported_comments = 0

    for _, row in comm.iterrows():
        msg = str(row.get("message", "") or "").strip()
        master_ext = row.get("masterID")
        req_ext = row.get("requestID")

        if not msg:
            continue
        if req_ext is None or (isinstance(req_ext, float) and pd.isna(req_ext)):
            continue

        internal_req_id = request_map.get(int(req_ext))
        if not internal_req_id:
            continue

        author_internal = None
        if master_ext is not None and not (isinstance(master_ext, float) and pd.isna(master_ext)):
            author_internal = user_map.get(int(master_ext))

        if not author_internal:
            # если не нашли автора — берём первого пользователя
            author_internal = list(user_map.values())[0]

        cur.execute("""
            INSERT INTO request_comments (request_id, user_id, comment, is_ordered_parts, parts_description)
            VALUES (?, ?, ?, 0, '')
        """, (internal_req_id, author_internal, msg))
        imported_comments += 1

    conn.commit()
    print(f"Комментариев импортировано: {imported_comments}")

    conn.close()
    print("Готово. База:", DB_PATH)

if __name__ == "__main__":
    main()
