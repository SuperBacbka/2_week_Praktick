import io
from datetime import datetime, timedelta, time as dtime

import pandas as pd
import plotly.express as px
import streamlit as st

from database import Database



QR_FEEDBACK_URL = "https://docs.google.com/forms/d/e/XXXXXXXXXXXX/viewform"

QR_INCLUDE_REQUEST_PARAM = True


# -------------------- UI SETUP --------------------
st.set_page_config(
    page_title="Сервисный центр (климатическое оборудование)",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header { font-size: 2.2rem; font-weight: 800; color: #1565C0; }
    .subtle { color: #607D8B; font-size: 0.95rem; }
    .badge { display: inline-block; padding: 0.2rem 0.55rem; border-radius: 0.5rem; font-weight: 700;  color:#111 !important;}
    
    .b-open { background: #FFF3E0; border: 1px solid #FFB74D; }
    .b-prog { background: #E3F2FD; border: 1px solid #64B5F6; }
    .b-wait { background: #F3E5F5; border: 1px solid #BA68C8; }
    .b-done { background: #E8F5E9; border: 1px solid #81C784; }
</style>
""", unsafe_allow_html=True)


# -------------------- DB INIT --------------------

@st.cache_resource
def init_db():
    return Database()


if "db" not in st.session_state:
    st.session_state.db = init_db()

if "user" not in st.session_state:
    st.session_state.user = None

if "page" not in st.session_state:
    st.session_state.page = "Вход"


# -------------------- HELPERS --------------------
def role() -> str:
    return (st.session_state.user or {}).get("role", "")

def user_id() -> int:
    return int((st.session_state.user or {}).get("id", 0) or 0)

def is_admin() -> bool:
    return role() == "admin"

def is_specialist() -> bool:
    return role() == "specialist"

def is_quality_manager() -> bool:
    return role() == "quality_manager"

def can_qm_actions() -> bool:
    return is_quality_manager() or is_admin()

def status_badge(status: str) -> str:
    css = {
        "открыта": "badge b-open",
        "в процессе ремонта": "badge b-prog",
        "ожидание комплектующих": "badge b-wait",
        "завершена": "badge b-done",
    }.get(status, "badge b-open")
    return f'<span class="{css}">{status}</span>'

def effective_deadline(r: dict) -> str:
    return r.get("deadline_extended_to") or r.get("deadline") or ""

def deadline_state(r: dict) -> str:
    """
    Небольшой индикатор риска: если дедлайн есть и до него < 24ч (и не завершена).
    """
    if r.get("status") == "завершена":
        return ""
    dl = effective_deadline(r)
    if not dl:
        return ""
    try:
        dt = datetime.fromisoformat(str(dl).replace(" ", "T"))
        delta = dt - datetime.now()
        if delta.total_seconds() < 0:
            return "Просрочено"
        if delta.total_seconds() <= 24 * 3600:
            return "Риск срыва"
        return "В сроке"
    except Exception:
        return ""

def render_qr(url: str):
    # 1) Streamlit qr_code (если доступно)
    if hasattr(st, "qr_code"):
        st.qr_code(url)
        st.caption("Отсканируйте QR-код для отзыва")
        st.link_button("Открыть форму", url)
        return

    # 2) Пакет qrcode (если установлен)
    try:
        import qrcode
        img = qrcode.make(url)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        st.image(buf.getvalue(), width=240)
        st.caption("Отсканируйте QR-код для отзыва")
        st.link_button("Открыть форму", url)
        return
    except Exception:
        pass

    st.info("QR недоступен в текущей среде. Ссылка на форму:")
    st.write(url)


# -------------------- PAGES --------------------
def page_login():
    st.markdown('<div class="main-header">❄️ Учет заявок на ремонт климатического оборудования</div>', unsafe_allow_html=True)
    st.write('<div class="subtle">Streamlit + SQLite. Роли: admin / specialist / quality_manager.</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.container(border=True):
            st.subheader("🔐 Вход")
            with st.form("login_form"):
                u = st.text_input("Логин")
                p = st.text_input("Пароль", type="password")
                ok = st.form_submit_button("Войти", type="primary")
            if ok:
                user = st.session_state.db.authenticate_user(u, p)
                if user:
                    st.session_state.user = user
                    st.session_state.page = "Дашборд"
                    st.rerun()
                else:
                    st.error("Неверный логин или пароль")

            st.divider()
            st.write("**Тестовые учетные данные:**")
            st.write("- admin / admin123")
            st.write("- ivanov / spec123")
            st.write("- qmanager / qm123")


def page_dashboard():
    st.title("📊 Дашборд")
    stats = st.session_state.db.get_statistics(30)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего заявок (30 дн.)", stats["total_requests"])
    c2.metric("Завершено", stats["completed_requests"])
    c3.metric("В работе", stats["in_progress_requests"])
    c4.metric("Выполнение", f'{stats["completion_rate"]}%')

    st.divider()

    st.subheader("Быстрые действия")
    b1, b2, b3, b4 = st.columns(4)

    with b1:
        if (is_admin() or is_specialist()) and st.button("Новая заявка", type="primary", use_container_width=True):
            st.session_state.page = "Новая заявка"
            st.rerun()

    with b2:
        if st.button("📋 Все заявки", type="primary", use_container_width=True):
            st.session_state.page = "Все заявки"
            st.rerun()

    with b3:
        if can_qm_actions() and st.button("🧪 Контроль качества", type="primary", use_container_width=True):
            st.session_state.page = "Контроль качества"
            st.rerun()

    with b4:
        if st.button("📈 Статистика", type="primary", use_container_width=True):
            st.session_state.page = "Статистика"
            st.rerun()


def page_new_request():
    if not (is_admin() or is_specialist()):
        st.error("Доступ запрещен.")
        return

    st.title("Новая заявка")

    eq_types = st.session_state.db.get_equipment_types()
    eq_options = [e["name"] for e in eq_types] if eq_types else []

    fault_options = [
        "Не охлаждает / не греет",
        "Течёт вода",
        "Шум / вибрации",
        "Не включается",
        "Ошибка на дисплее",
        "Запах / загрязнение",
        "Низкое давление / утечка",
        "Другое",
    ]

    with st.form("new_req_form", clear_on_submit=True):
        c1, c2 = st.columns(2)

        with c1:
            equipment_type = st.selectbox("Тип оборудования*", eq_options)
            device_model = st.text_input("Модель*")
            fault_type = st.selectbox("Тип неисправности", ["Не указано"] + fault_options)
            customer_name = st.text_input("ФИО заказчика*")
            customer_phone = st.text_input("Телефон заказчика*")

        with c2:
            problem_description = st.text_area("Описание проблемы*", height=160)
            estimated_cost = st.number_input("Предварительная стоимость (руб.)", min_value=0.0, step=100.0)

        st.caption("Плановый срок (deadline) задаётся автоматически как +3 дня. Менеджер качества может продлить.")
        submit = st.form_submit_button("Создать", type="primary")

    if submit:
        if not all([equipment_type, device_model, customer_name, customer_phone, problem_description]):
            st.error("Заполните все обязательные поля (*)")
            return

        data = {
            "equipment_type": equipment_type,
            "device_model": device_model.strip(),
            "fault_type": "" if fault_type == "Не указано" else fault_type,
            "problem_description": problem_description.strip(),
            "customer_name": customer_name.strip(),
            "customer_phone": customer_phone.strip(),
            "estimated_cost": float(estimated_cost or 0),
        }
        rid = st.session_state.db.add_request(data)
        if rid:
            st.success("Заявка создана.")
            st.session_state.view_request_id = rid
            st.session_state.page = "Просмотр заявки"
            st.rerun()
        else:
            st.error("Ошибка при создании заявки")


def page_all_requests():
    st.title("📋 Все заявки")

    with st.expander("🔍 Фильтры", expanded=True):
        f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.6, 1.0])

        with f1:
            status_options = ["Все", "открыта", "в процессе ремонта", "ожидание комплектующих", "завершена"]
            status_sel = st.selectbox("Статус", status_options)

        with f2:
            specialists = st.session_state.db.get_all_users("specialist")
            spec_options = ["Все"] + [f'{s["id"]} - {s["full_name"]}' for s in specialists]
            spec_sel = st.selectbox("Исполнитель", spec_options)

        with f3:
            search = st.text_input("Поиск (номер, ФИО, телефон)")

        with f4:
            view_mode = st.radio("Вид", ["Карточки", "Таблица"], horizontal=True)

        d1, d2 = st.columns(2)
        with d1:
            date_from = st.date_input("С", value=datetime.now() - timedelta(days=30))
        with d2:
            date_to = st.date_input("По", value=datetime.now())

    filters = {}
    if status_sel != "Все":
        filters["status"] = status_sel
    if spec_sel != "Все" and " - " in spec_sel:
        filters["assigned_to"] = int(spec_sel.split(" - ")[0])
    if search.strip():
        filters["search"] = search.strip()
    filters["date_from"] = date_from.strftime("%Y-%m-%d")
    filters["date_to"] = date_to.strftime("%Y-%m-%d")

    items = st.session_state.db.get_requests(filters)

    if not items:
        st.info("Заявки не найдены.")
        return

    if view_mode == "Карточки":
        for r in items:
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([2.0, 2.2, 1.5, 1.0])

                with c1:
                    st.write(f"**{r['request_number']}**  {deadline_state(r)}")
                    st.write(f"Дата: {str(r['created_date'])[:16] if r.get('created_date') else ''}")
                    st.write(f"Оборудование: {r.get('equipment_type','')} | Модель: {r.get('device_model','')}")
                    if r.get("fault_type"):
                        st.write(f"Неисправность: {r.get('fault_type')}")

                with c2:
                    st.write(f"Заказчик: {r.get('customer_name','')}")
                    st.write(f"Телефон: {r.get('customer_phone','')}")
                    st.write(f"Срок (план): {r.get('deadline') or '—'}")
                    if r.get("deadline_extended_to"):
                        st.write(f"Срок (продлён): {r.get('deadline_extended_to')}")

                with c3:
                    st.markdown(f"Статус: {status_badge(r.get('status',''))}", unsafe_allow_html=True)
                    st.write(f"Мастер: {r.get('assigned_name') or '—'}")
                    if r.get("assist_name"):
                        st.write(f"Помощник: {r.get('assist_name')}")
                    if r.get("estimated_cost") is not None:
                        st.write(f"Оценка: {float(r['estimated_cost']):,.0f} ₽")

                with c4:
                    if st.button("👁️", key=f"view_{r['id']}", help="Просмотр"):
                        st.session_state.view_request_id = int(r["id"])
                        st.session_state.page = "Просмотр заявки"
                        st.rerun()
                    if (is_admin() or is_specialist()) and st.button("✏️", key=f"edit_{r['id']}", help="Редактировать"):
                        st.session_state.edit_request_id = int(r["id"])
                        st.session_state.page = "Редактирование заявки"
                        st.rerun()

    else:
        df = pd.DataFrame([{
            "ID": r["id"],
            "Номер": r["request_number"],
            "Дата": str(r["created_date"])[:10] if r.get("created_date") else "",
            "Оборудование": r.get("equipment_type", ""),
            "Модель": r.get("device_model", ""),
            "Неисправность": r.get("fault_type", "") or "—",
            "Заказчик": r.get("customer_name", ""),
            "Телефон": r.get("customer_phone", ""),
            "Статус": r.get("status", ""),
            "Мастер": r.get("assigned_name", "") or "—",
            "Срок": effective_deadline(r) or "—",
            "Риск": deadline_state(r) or "",
        } for r in items])
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.success(f"Найдено: {len(items)}")


def page_view_request():
    rid = st.session_state.get("view_request_id")
    if not rid:
        st.error("Заявка не выбрана.")
        st.session_state.page = "Все заявки"
        st.rerun()
        return

    r = st.session_state.db.get_request(int(rid))
    if not r:
        st.error("Заявка не найдена.")
        st.session_state.page = "Все заявки"
        st.rerun()
        return

    st.title(f"📄 Заявка {r['request_number']}")

    left, right = st.columns([2.2, 1.2])
    with left:
        with st.container(border=True):
            st.subheader("📌 Данные заявки")
            st.write(f"**Дата создания:** {r.get('created_date')}")
            st.write(f"**Статус:** {r.get('status')}")
            st.write(f"**Оборудование:** {r.get('equipment_type')} | **Модель:** {r.get('device_model')}")
            if r.get("fault_type"):
                st.write(f"**Тип неисправности:** {r.get('fault_type')}")
            st.write(f"**Заказчик:** {r.get('customer_name')} | **Телефон:** {r.get('customer_phone')}")
            st.write("**Описание проблемы:**")
            st.write(r.get("problem_description") or "")

            st.divider()
            st.write(f"**Мастер:** {r.get('assigned_name') or '—'}")
            st.write(f"**Помощник:** {r.get('assist_name') or '—'}")

            st.divider()
            st.write(f"**Срок (план):** {r.get('deadline') or '—'}")
            if r.get("deadline_extended_to"):
                st.write(f"**Срок (продлён):** {r.get('deadline_extended_to')}")
                st.write(f"**Причина продления:** {r.get('extension_reason') or '—'}")
                st.write(f"**Согласование клиента:** {r.get('client_approval') or '—'}")
                st.write(f"**Когда согласовано:** {r.get('client_approval_at') or '—'}")
                st.write(f"**Кто продлил:** {r.get('extended_by_name') or '—'}")

            st.write(f"**Индикатор:** {deadline_state(r) or '—'}")

            if r.get("estimated_cost") is not None:
                st.write(f"**Оценка:** {float(r['estimated_cost']):,.0f} ₽")
            if r.get("actual_cost") is not None:
                st.write(f"**Факт:** {float(r['actual_cost']):,.0f} ₽")

    with right:
        with st.container(border=True):
            st.subheader("⚙️ Действия")

            # права на изменение статуса: admin или назначенный мастер
            assigned_to = r.get("assigned_to")
            can_manage = is_admin() or (is_specialist() and assigned_to and int(assigned_to) == user_id())

            if can_manage:
                statuses = ["открыта", "в процессе ремонта", "ожидание комплектующих", "завершена"]
                cur = r.get("status") or "открыта"
                sel = st.selectbox("Изменить статус", statuses, index=statuses.index(cur) if cur in statuses else 0)

                if sel != cur:
                    if st.button("Сохранить статус", type="primary", use_container_width=True):
                        ok = st.session_state.db.update_request_status(int(r["id"]), sel, user_id())
                        if ok:
                            st.success("Статус обновлён.")
                            st.rerun()
                        else:
                            st.error("Не удалось обновить статус.")

                if is_admin():
                    st.divider()
                    st.subheader("👷 Назначение мастера")
                    specialists = st.session_state.db.get_all_users("specialist")
                    opts = ["—"] + [f'{s["id"]} - {s["full_name"]}' for s in specialists]
                    chosen = st.selectbox("Основной мастер", opts, key=f"assign_main_{r['id']}")
                    if st.button("Назначить", use_container_width=True):
                        if chosen == "—":
                            st.error("Выберите мастера.")
                        else:
                            sid = int(chosen.split(" - ")[0])
                            ok = st.session_state.db.assign_request(int(r["id"]), sid)
                            st.success("Назначено.") if ok else st.error("Не удалось назначить.")
                            st.rerun()

            # кнопка "Нужна помощь" по ТЗ: мастер в заявке нажимает
            if is_specialist() and assigned_to and int(assigned_to) == user_id() and r.get("status") != "завершена":
                st.divider()
                st.subheader("🆘 Нужна помощь")
                with st.form(f"help_form_{r['id']}"):
                    msg = st.text_area("Опишите, почему требуется помощь/эскалация")
                    sent = st.form_submit_button("Отправить запрос менеджеру качества", type="primary")
                if sent:
                    hid = st.session_state.db.create_help_request(int(r["id"]), user_id(), msg.strip())
                    if hid:
                        st.success("Запрос помощи отправлен.")
                    else:
                        st.error("Не удалось отправить запрос.")

            st.divider()
            if st.button("📋 К списку заявок", type="secondary", use_container_width=True):
                st.session_state.page = "Все заявки"
                st.rerun()

            if (is_admin() or is_specialist()) and st.button("✏️ Редактировать", type="secondary", use_container_width=True):
                st.session_state.edit_request_id = int(r["id"])
                st.session_state.page = "Редактирование заявки"
                st.rerun()

    st.divider()

    # -------------------- QR --------------------
    st.subheader("⭐ Отзыв (QR)")
    feedback_url = QR_FEEDBACK_URL
    if QR_INCLUDE_REQUEST_PARAM and r.get("request_number"):
        glue = "&" if "?" in feedback_url else "?"
        feedback_url = f"{feedback_url}{glue}request={r['request_number']}"
    render_qr(feedback_url)

    st.divider()

    # -------------------- COMMENTS --------------------
    st.subheader("💬 Комментарии")

    if st.session_state.user:
        with st.form(f"comment_form_{r['id']}"):
            comment = st.text_area("Комментарий")
            c1, c2 = st.columns(2)
            with c1:
                is_parts = st.checkbox("Заказаны комплектующие")
            with c2:
                parts_desc = st.text_input("Описание комплектующих") if is_parts else ""
            add = st.form_submit_button("Добавить", type="primary")
        if add:
            if comment.strip() or parts_desc.strip():
                cid = st.session_state.db.add_comment(
                    int(r["id"]), user_id(), comment.strip(), is_parts, parts_desc.strip()
                )
                if cid:
                    st.success("Комментарий добавлен.")
                    st.rerun()
                else:
                    st.error("Не удалось добавить комментарий.")
    else:
        st.info("Войдите в систему, чтобы добавлять комментарии.")

    comments = st.session_state.db.get_request_comments(int(r["id"]))
    if comments:
        for c in comments:
            with st.container(border=True):
                st.write(f"**{c.get('author_name','')}**  ·  {c.get('created_at','')}")
                if c.get("comment"):
                    st.write(c["comment"])
                if int(c.get("is_ordered_parts") or 0) == 1:
                    st.warning(f"🧰 Комплектующие заказаны: {c.get('parts_description') or '—'}")
    else:
        st.info("Комментариев нет.")

    st.divider()
    st.subheader("📊 История статусов")
    hist = st.session_state.db.get_status_history(int(r["id"]))
    if hist:
        for h in hist:
            with st.container(border=True):
                st.write(f"{h.get('changed_at')} · **{h.get('old_status')} → {h.get('new_status')}**")
                if h.get("changed_by_name"):
                    st.caption(f"Кем: {h.get('changed_by_name')}")
    else:
        st.info("История отсутствует.")


def page_edit_request():
    if not (is_admin() or is_specialist()):
        st.error("Доступ запрещен.")
        return

    rid = st.session_state.get("edit_request_id")
    if not rid:
        st.error("Заявка не выбрана.")
        st.session_state.page = "Все заявки"
        st.rerun()
        return

    r = st.session_state.db.get_request(int(rid))
    if not r:
        st.error("Заявка не найдена.")
        st.session_state.page = "Все заявки"
        st.rerun()
        return

    st.title(f"✏️ Редактирование {r['request_number']}")

    cur_status = r.get("status") or "открыта"

    eq_types = st.session_state.db.get_equipment_types()
    eq_options = [e["name"] for e in eq_types] if eq_types else []
    fault_options = [
        "Не охлаждает / не греет",
        "Течёт вода",
        "Шум / вибрации",
        "Не включается",
        "Ошибка на дисплее",
        "Запах / загрязнение",
        "Низкое давление / утечка",
        "Другое",
    ]

    with st.form("edit_form"):
        c1, c2 = st.columns(2)

        with c1:
            customer_name = st.text_input("ФИО", value=r.get("customer_name") or "")
            customer_phone = st.text_input("Телефон", value=r.get("customer_phone") or "")
            equipment_type = st.selectbox("Тип оборудования", eq_options, index=eq_options.index(r.get("equipment_type")) if r.get("equipment_type") in eq_options else 0)
            device_model = st.text_input("Модель", value=r.get("device_model") or "")

        with c2:
            fault_type = st.selectbox(
                "Тип неисправности",
                ["Не указано"] + fault_options,
                index=(["Не указано"] + fault_options).index(r.get("fault_type")) if r.get("fault_type") in (["Не указано"] + fault_options) else 0
            )
            problem_description = st.text_area("Описание", value=r.get("problem_description") or "", height=150)
            estimated_cost = st.number_input("Оценка (руб.)", min_value=0.0, step=100.0, value=float(r.get("estimated_cost") or 0))
            actual_cost = st.number_input("Факт (руб.)", min_value=0.0, step=100.0, value=float(r.get("actual_cost") or 0))

        st.divider()
        statuses = ["открыта", "в процессе ремонта", "ожидание комплектующих", "завершена"]
        sel_status = st.selectbox("Статус", statuses, index=statuses.index(cur_status) if cur_status in statuses else 0)

        # (опционально) ручной плановый срок — пусть изменяет только админ/км
        if can_qm_actions():
            st.caption("Плановый срок можно корректировать. Продление по ТЗ — через страницу контроля качества.")
            # Только редактируем базовый deadline (не продление)
            cur_deadline = r.get("deadline") or ""
            new_deadline = st.text_input("Плановый срок (YYYY-MM-DD HH:MM:SS)", value=str(cur_deadline)[:19] if cur_deadline else "")

        save = st.form_submit_button("Сохранить", type="primary")

    if save:
        upd = {
            "customer_name": customer_name.strip(),
            "customer_phone": customer_phone.strip(),
            "equipment_type": equipment_type,
            "device_model": device_model.strip(),
            "fault_type": "" if fault_type == "Не указано" else fault_type,
            "problem_description": problem_description.strip(),
            "estimated_cost": float(estimated_cost or 0),
            "actual_cost": float(actual_cost or 0),
        }
        if can_qm_actions():
            if new_deadline.strip():
                upd["deadline"] = new_deadline.strip()

        ok = st.session_state.db.update_request(int(r["id"]), upd)
        if not ok:
            st.error("Не удалось сохранить изменения.")
            return

        if sel_status != cur_status:
            ok2 = st.session_state.db.update_request_status(int(r["id"]), sel_status, user_id())
            if not ok2:
                st.error("Поля сохранены, но статус обновить не удалось.")
                return

        st.success("Сохранено.")
        st.session_state.view_request_id = int(r["id"])
        st.session_state.page = "Просмотр заявки"
        st.rerun()


def page_quality_control():
    if not can_qm_actions():
        st.error("Доступ запрещен.")
        return

    st.title("🧪 Контроль качества")

    items = st.session_state.db.list_open_help_requests()
    if not items:
        st.info("Открытых запросов помощи нет.")
        return

    specialists = st.session_state.db.get_all_users("specialist")
    spec_options = ["—"] + [f'{s["id"]} - {s["full_name"]}' for s in specialists]

    for hr in items:
        with st.container(border=True):
            st.write(f"**Запрос помощи #{hr['help_id']}**")
            st.write(f"Заявка: **{hr['request_number']}** (ID {hr['request_id']})")
            st.write(f"Статус заявки: {hr.get('status')}")
            st.write(f"Текущий мастер: {hr.get('assigned_name') or '—'}")
            st.write(f"Запросил: {hr.get('requested_by_name')} · {hr.get('created_at')}")
            if hr.get("message"):
                st.info(hr["message"])

            st.divider()
            c1, c2 = st.columns(2)

            # Подключить/переназначить
            with c1:
                st.subheader("👷 Привлечь мастера")
                mode = st.selectbox(
                    "Модель решения",
                    ["Назначить помощника", "Переназначить основного мастера"],
                    key=f"mode_{hr['help_id']}"
                )
                sel = st.selectbox("Выберите специалиста", spec_options, key=f"spec_{hr['help_id']}")
                if st.button("Применить", key=f"apply_{hr['help_id']}", type="primary", use_container_width=True):
                    if sel == "—":
                        st.error("Выберите специалиста.")
                    else:
                        sid = int(sel.split(" - ")[0])
                        if mode == "Назначить помощника":
                            ok = st.session_state.db.set_assistant_master(int(hr["request_id"]), sid)
                        else:
                            ok = st.session_state.db.reassign_master(int(hr["request_id"]), sid)
                        st.success("Решение применено.") if ok else st.error("Не удалось применить.")

            # Продление срока с согласованием
            with c2:
                st.subheader("📅 Продлить срок")
                nd = st.date_input("Новая дата", key=f"date_{hr['help_id']}")
                nt = st.time_input("Новое время", value=dtime(18, 0), key=f"time_{hr['help_id']}")
                reason = st.text_area("Причина продления*", key=f"reason_{hr['help_id']}")
                approval = st.text_input("Согласование клиента (канал/контакт/кто)*", key=f"approval_{hr['help_id']}")
                approved = st.checkbox("Подтверждаю, что согласование получено", key=f"approved_{hr['help_id']}")

                if st.button("Продлить", key=f"extend_{hr['help_id']}", use_container_width=True):
                    if not approved:
                        st.error("Нужно подтвердить факт согласования.")
                    elif not reason.strip():
                        st.error("Укажите причину продления.")
                    elif not approval.strip():
                        st.error("Заполните поле согласования клиента.")
                    else:
                        dt = datetime.combine(nd, nt).strftime("%Y-%m-%d %H:%M:%S")
                        ok = st.session_state.db.extend_deadline(
                            int(hr["request_id"]),
                            dt,
                            reason.strip(),
                            approval.strip(),
                            user_id()
                        )
                        st.success("Срок продлён.") if ok else st.error("Не удалось продлить срок.")

            st.divider()
            note = st.text_area("Решение / примечание менеджера", key=f"note_{hr['help_id']}")
            if st.button("Закрыть запрос помощи", key=f"close_{hr['help_id']}", type="primary", use_container_width=True):
                ok = st.session_state.db.resolve_help_request(int(hr["help_id"]), user_id(), note.strip())
                st.success("Запрос закрыт.") if ok else st.error("Не удалось закрыть запрос.")
                st.rerun()


def page_specialists():
    if not is_admin():
        st.error("Доступ запрещен.")
        return

    st.title("👥 Пользователи")

    with st.expander("➕ Добавить пользователя", expanded=False):
        with st.form("add_user_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                username = st.text_input("Логин*")
                password = st.text_input("Пароль*", type="password")
                full_name = st.text_input("ФИО*")
            with c2:
                role_sel = st.selectbox("Роль", ["specialist", "quality_manager", "admin"])
                phone = st.text_input("Телефон")
                email = st.text_input("Email")

            add = st.form_submit_button("Добавить", type="primary")

        if add:
            if not (username.strip() and password.strip() and full_name.strip()):
                st.error("Заполните обязательные поля.")
            else:
                uid = st.session_state.db.add_user({
                    "username": username.strip(),
                    "password": password,
                    "full_name": full_name.strip(),
                    "role": role_sel,
                    "phone": phone.strip(),
                    "email": email.strip()
                })
                st.success("Пользователь добавлен.") if uid else st.error("Не удалось добавить (возможно, логин занят).")
                st.rerun()

    st.divider()
    users = st.session_state.db.get_all_users()
    df = pd.DataFrame([{
        "ID": u["id"],
        "Логин": u["username"],
        "ФИО": u.get("full_name", ""),
        "Роль": u.get("role", ""),
        "Телефон": u.get("phone", ""),
        "Email": u.get("email", ""),
    } for u in users])
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_statistics():
    st.title("📈 Статистика")

    period = st.selectbox("Период", ["7 дней", "30 дней", "90 дней", "Все время"])
    days = {"7 дней": 7, "30 дней": 30, "90 дней": 90, "Все время": 3650}[period]

    stats = st.session_state.db.get_statistics(days)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего", stats["total_requests"])
    c2.metric("Завершено", stats["completed_requests"])
    c3.metric("Открыто", stats["open_requests"])
    c4.metric("Среднее время ремонта", f'{stats["avg_completion_days"]} дн.')

    st.divider()

    # Pie по статусам
    status_data = {
        "Открыта": stats["open_requests"],
        "В процессе": stats["in_progress_requests"],
        "Завершена": stats["completed_requests"],
    }
    if any(status_data.values()):
        fig = px.pie(values=list(status_data.values()), names=list(status_data.keys()), title="Статусы заявок")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    colA, colB = st.columns(2)

    with colA:
        st.subheader("ТОП типов оборудования")
        eq = stats["equipment_stats"]
        if eq:
            df = pd.DataFrame(eq).head(10)
            fig2 = px.bar(df, x="name", y="cnt", title="Оборудование", labels={"name": "Тип", "cnt": "Количество"})
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Нет данных.")

    with colB:
        st.subheader("ТОП типов неисправностей")
        fs = stats["fault_stats"]
        if fs:
            df = pd.DataFrame(fs).head(10)
            fig3 = px.bar(df, x="name", y="cnt", title="Неисправности", labels={"name": "Тип", "cnt": "Количество"})
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Нет данных.")

    st.divider()
    st.subheader("Эффективность специалистов (завершённые)")
    ss = stats["specialist_stats"]
    if ss:
        df = pd.DataFrame(ss)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("Нет данных.")


# -------------------- SIDEBAR NAV --------------------
def sidebar_nav():
    with st.sidebar:
        st.markdown("### ❄️ Климат-Сервис")
        if st.session_state.user:
            st.success(st.session_state.user.get("full_name") or st.session_state.user.get("username"))
            st.caption(f"Роль: {role()}")
        st.divider()

        if not st.session_state.user:
            st.session_state.page = "Вход"
            return

        items = ["📊 Дашборд", "📋 Все заявки", "📈 Статистика", "🚪 Выход"]

        # Новая заявка — только admin/specialist
        if is_admin() or is_specialist():
            items.insert(1, "➕ Новая заявка")

        # Контроль качества — admin/quality_manager
        if can_qm_actions():
            items.insert(2, "🧪 Контроль качества")

        # Пользователи — только admin
        if is_admin():
            items.insert(3, "👥 Пользователи")

        # подстраницы — чтобы selectbox не сбрасывал page
        if st.session_state.page == "Просмотр заявки" and "📄 Просмотр заявки" not in items:
            items.insert(2, "📄 Просмотр заявки")
        if st.session_state.page == "Редактирование заявки" and "✏️ Редактирование заявки" not in items:
            items.insert(2, "✏️ Редактирование заявки")

        page_map = {
            "📊 Дашборд": "Дашборд",
            "➕ Новая заявка": "Новая заявка",
            "📋 Все заявки": "Все заявки",
            "📄 Просмотр заявки": "Просмотр заявки",
            "✏️ Редактирование заявки": "Редактирование заявки",
            "🧪 Контроль качества": "Контроль качества",
            "👥 Пользователи": "Пользователи",
            "📈 Статистика": "Статистика",
            "🚪 Выход": "Выход",
        }

        label_by_page = {v: k for k, v in page_map.items()}
        current_label = label_by_page.get(st.session_state.page, "📊 Дашборд")
        if current_label not in items:
            current_label = items[0]

        selected = st.selectbox("Навигация", items, index=items.index(current_label))

        if selected == "🚪 Выход":
            st.session_state.user = None
            st.session_state.page = "Вход"
            st.rerun()
        else:
            st.session_state.page = page_map[selected]


# -------------------- ROUTER --------------------
def router():
    sidebar_nav()

    p = st.session_state.page
    if p == "Вход":
        page_login()
    elif p == "Дашборд":
        page_dashboard()
    elif p == "Новая заявка":
        page_new_request()
    elif p == "Все заявки":
        page_all_requests()
    elif p == "Просмотр заявки":
        page_view_request()
    elif p == "Редактирование заявки":
        page_edit_request()
    elif p == "Контроль качества":
        page_quality_control()
    elif p == "Пользователи":
        page_specialists()
    elif p == "Статистика":
        page_statistics()
    else:
        st.error("Страница не найдена.")
        st.session_state.page = "Дашборд"
        st.rerun()


if __name__ == "__main__":
    router()
