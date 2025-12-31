import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time, date, timedelta
import requests
import uuid
import bcrypt
import holidays
from streamlit_calendar import calendar
import extra_streamlit_components as stx
import time as tm

# --- פונקציה לטעינת ה-CSS ---
def load_css(file_name):
    try:
        with open(file_name, encoding='utf-8') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError: pass

# --- הגדרות וקבועים ---
SHEET_ID = '1Uf_bLdIKz8aJAc1BV1OZvQwNP5Rzn4LqnQSuhL9htjg' 
DATE_FMT = '%Y-%m-%d'
TIME_FMT = '%H:%M'
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"
STATUS_ACTIVE = "active"

# --- ניהול עוגיות (Cookie Manager) ---
def get_cookie_manager():
    return stx.CookieManager(key="auth_cookie_manager")

cookie_manager = get_cookie_manager()

# --- 1. אבטחה והצפנה ---
def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed.encode())

# --- 2. חיבור לגוגל שיטס ---
@st.cache_resource
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"שגיאה בהתחברות לגוגל: {e}")
        st.stop()

def get_worksheet(name):
    client = get_gspread_client()
    try:
        sh = client.open_by_key(SHEET_ID)
        return sh.worksheet(name)
    except Exception as e:
        st.error(f"שגיאה בטעינת הגיליון '{name}': {e}")
        st.stop()

# --- 3. כלי עזר (טלגרם ודאטה) ---
def send_telegram(message):
    try:
        token = st.secrets["general"]["telegram_token"]
        chat_id = st.secrets["general"]["telegram_chat_id"]
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": message})
    except: pass 

@st.cache_data(ttl=60)
def get_data(sheet_name):
    ws = get_worksheet(sheet_name)
    return pd.DataFrame(ws.get_all_records())

def update_status_safe(sheet_name, id_col, item_id, status_col_idx, new_status):
    ws = get_worksheet(sheet_name)
    df = pd.DataFrame(ws.get_all_records())
    try:
        df[id_col] = df[id_col].astype(str)
        row_idx = df[df[id_col] == str(item_id)].index[0] + 2
        ws.update_cell(row_idx, status_col_idx, new_status)
        st.cache_data.clear()
        return True
    except: return False

# --- 4. לוגיקה עסקית ---
def register_user(full_name, phone, apt, role, password):
    ws = get_worksheet("Users")
    users = get_data("Users")
    clean_phone = phone.strip()
    
    if not users.empty and str(clean_phone) in users['Phone'].astype(str).values:
        return False, "הטלפון כבר קיים במערכת"

    hashed_pw = hash_password(password)
    ws.append_row([full_name, f"'{clean_phone}", str(apt), role, hashed_pw, STATUS_PENDING, "user"])
    st.cache_data.clear()
    
    send_telegram(f"🔔 *הרשמה חדשה*\nשם: {full_name}\nדירה: {apt}\nטלפון: {phone}")
    return True, "בקשת ההרשמה נשלחה למנהל המערכת לאישור."

def login_user(phone, password):
    users = get_data("Users")
    clean_input = str(phone).strip().replace("-", "").replace(" ", "")
    
    if users.empty: return None
    
    users['CleanPhone'] = users['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
    user_row = users[users['CleanPhone'] == clean_input]
    
    if user_row.empty: return None
    
    stored_hash = user_row.iloc[0]['Password']
    if verify_password(password, stored_hash):
        return user_row.iloc[0].to_dict()
    return None

def check_overlap(date_str, start_str, end_str):
    bookings = get_data("Bookings")
    if bookings.empty: return False
    
    active = bookings[(bookings['Date'] == date_str) & (bookings['Status'].isin([STATUS_APPROVED, STATUS_PENDING]))]
    if active.empty: return False
    
    new_start = datetime.strptime(start_str, TIME_FMT).time()
    new_end = datetime.strptime(end_str, TIME_FMT).time()
    
    for _, row in active.iterrows():
        ex_start = datetime.strptime(row['Start Time'], TIME_FMT).time()
        ex_end = datetime.strptime(row['End Time'], TIME_FMT).time()
        if new_start < ex_end and new_end > ex_start:
            return True
    return False

def add_booking(user_data, date_obj, start, end):
    if start >= end: return False, "שעת הסיום חייבת להיות אחרי שעת ההתחלה"
    
    date_str = date_obj.strftime(DATE_FMT)
    start_str = start.strftime(TIME_FMT)
    end_str = end.strftime(TIME_FMT)
    
    if check_overlap(date_str, start_str, end_str):
        return False, "החדר תפוס (או ממתין לאישור) בשעות אלו"
        
    ws = get_worksheet("Bookings")
    b_id = str(uuid.uuid4())[:8]
    ws.append_row([b_id, f"'{user_data['Phone']}", user_data['Full Name'], date_str, start_str, end_str, STATUS_PENDING, str(user_data['Apt'])])
    
    st.cache_data.clear()
    send_telegram(f"📅 *בקשה לשיריון*\nדייר: {user_data['Full Name']} (דירה {user_data['Apt']})\nתאריך: {date_str}\nשעות: {start_str}-{end_str}")
    return True, "הבקשה נשלחה למנהל המערכת לאישור."

def get_calendar_events():
    events = []
    apt_colors = { "13": "#FF5733", "1": "#33FF57", "5": "#3357FF" }
    default_color = "#3E3080"

    try:
        for date_obj, name in holidays.IL(years=datetime.now().year).items():
            events.append({
                "title": f"🇮🇱 {name}", "start": str(date_obj), "end": str(date_obj),
                "allDay": True, "backgroundColor": "#FFEB3B", "textColor": "#000000", "borderColor": "#FBC02D"
            })
    except: pass

    df = get_data("Bookings")
    if not df.empty:
        approved = df[df['Status'] == STATUS_APPROVED]
        for _, row in approved.iterrows():
            current_apt = str(row.get('Apt', '?'))
            chosen_dot_color = apt_colors.get(current_apt, default_color)
            event_title = f"דירה {current_apt}\n{row['Start Time']} - {row['End Time']}"
            
            events.append({
                "title": event_title,
                "start": f"{row['Date']}T{row['Start Time']}",
                "end": f"{row['Date']}T{row['End Time']}",
                "backgroundColor": "#FFFFFF", 
                "borderColor": chosen_dot_color, 
                "textColor": "#080808"
            })
    return events

# --- האפליקציה הראשית ---
st.set_page_config(page_title="ניהול דיירים", layout="wide")

load_css("style.css")

if 'user' not in st.session_state: st.session_state.user = None

# === בדיקת עוגיות (Auto Login) ===
# כאן התיקון: אנחנו בודקים אם הרגע לחצנו על יציאה לפני שמנסים להתחבר שוב
if st.session_state.user is None:
    if st.session_state.get('logout_clicked', False):
        st.session_state.logout_clicked = False
    else:
        # אנו ממתינים רגע קטן כדי לוודא שה-Component נטען
        cookie_phone = cookie_manager.get(cookie="logged_user_phone")
        
        if cookie_phone:
            users_db = get_data("Users")
            if not users_db.empty:
                users_db['CleanPhone'] = users_db['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
                found_user = users_db[users_db['CleanPhone'] == str(cookie_phone)]
                
                if not found_user.empty:
                    st.session_state.user = found_user.iloc[0].to_dict()
                    st.rerun()

# --- מסך התחברות / הרשמה ---
if not st.session_state.user:
    st.title("🏡 פורטל הבניין")
    tab1, tab2 = st.tabs(["כניסה", "הרשמה"])
    
    with tab1:
        l_phone = st.text_input("טלפון נייד")
        l_pass = st.text_input("סיסמה", type="password")
        if st.button("התחבר"):
            if l_phone == "admin" and l_pass == "admin123":
                 st.session_state.user = {'Role': 'admin', 'Full Name': 'מנהל על', 'Apt': '0', 'Phone': 'admin'}
                 st.rerun()
            
            user = login_user(l_phone, l_pass)
            if user:
                if user['Status'] == STATUS_ACTIVE:
                    st.session_state.user = user
                    
                    # === שמירת עוגיה תקינה ===
                    clean_phone_cookie = str(l_phone).strip().replace("-", "").replace(" ", "")
                    expires = datetime.now() + timedelta(days=7)
                    
                    cookie_manager.set("logged_user_phone", clean_phone_cookie, expires_at=expires)
                    
                    tm.sleep(0.5) 
                    st.rerun()
                    
                elif user['Status'] == STATUS_PENDING:
                    st.warning("⏳ המשתמש ממתין לאישור מנהל")
                else:
                    st.error("🚫 משתמש חסום")
            else:
                st.error("פרטים שגויים")

    with tab2:
        st.info("מלא את הפרטים ונשלח בקשה למנהל")
        r_name = st.text_input("שם מלא")
        r_phone = st.text_input("טלפון")
        r_apt = st.number_input("מספר דירה", min_value=1, max_value=49, step=1)
        r_type = st.selectbox("אני...", ["בעל דירה", "שוכר"])
        r_pass = st.text_input("בחר סיסמה", type="password")
        
        if st.button("שלח בקשה להרשמה"):
            if r_name and r_phone and r_pass:
                ok, msg = register_user(r_name, r_phone, r_apt, r_type, r_pass)
                if ok: st.success(msg)
                else: st.error(msg)
            else:
                st.error("נא למלא את כל השדות")

# --- המערכת פנימה ---
else:
    user = st.session_state.user
    is_admin = user.get('Role') in ['admin', 'committee']
    
    st.sidebar.title(f"שלום, {user['Full Name']}")
    st.sidebar.write(f"דירה: {user['Apt']}")
    
    menu = st.sidebar.radio("תפריט", ["לוח שנה ושיריון", "השיריונים שלי", "ניהול"] if is_admin else ["לוח שנה ושיריון", "השיריונים שלי"])
    
    # === כפתור התנתק מתוקן ===
    if st.sidebar.button("התנתק"):
        cookie_manager.delete("logged_user_phone")
        st.session_state.logout_clicked = True # מסמנים שהתנתקנו
        st.session_state.user = None
        tm.sleep(0.5)
        st.rerun()

    # --- לוח שנה ---
    if menu == "לוח שנה ושיריון":
        st.header("📅 יומן תפוסה ושיריון")
        col_form, col_calendar = st.columns([1, 3], gap="small")
        
        with col_form:
            with st.container(border=True):
                st.subheader("➕ שיריון מהיר")
                with st.form("new_book_flex"):
                    d = st.date_input("תאריך", min_value=datetime.today())
                    s = st.time_input("התחלה", time(18,0))
                    e = st.time_input("סיום", time(20,0))
                    
                    if st.form_submit_button("שלח בקשה"):
                        apt = user.get('Apt', '0')
                        ok, msg = add_booking(user, d, s, e)
                        if ok: st.success(msg)
                        else: st.error(msg)
            
        with col_calendar:
            calendar_opts = {
                "headerToolbar": {"left": "title", "center": "", "right": "dayGridMonth,timeGridWeek,prev,next"},
                "initialView": "dayGridMonth",
                "locale": "he",
                "direction": "rtl",
                "height": 650,
                "contentHeight": "auto"
            }
            
            calendar(events=get_calendar_events(), options=calendar_opts, custom_css="""
    .fc { background: white; padding: 10px; border-radius: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); color: #000000; }
    .fc-event-time { display: none !important; }
    .fc-event-title { white-space: pre-wrap !important; text-align: right; display: block; font-weight: bold; }
""")

    # --- השיריונים שלי ---
    elif menu == "השיריונים שלי":
        st.header("ההיסטוריה שלי")
        df = get_data("Bookings")
        if not df.empty:
            my_phone = str(user['Phone']).replace("'","")
            df['User Phone'] = df['User Phone'].astype(str).str.replace("'","")
            my_b = df[df['User Phone'] == my_phone]
            if not my_b.empty:
                st.dataframe(my_b[['Date', 'Start Time', 'End Time', 'Status']], use_container_width=True, hide_index=True)
            else:
                st.info("אין לך שיריונים")

    # --- ניהול ---
    elif menu == "ניהול" and is_admin:
        st.header("🛠️ פאנל ניהול")
        tab_users, tab_books = st.tabs(["משתמשים", "בקשות שיריון"])
        
        with tab_users:
            users = get_data("Users")
            pending = users[users['Status'] == STATUS_PENDING]
            if not pending.empty:
                st.subheader("ממתינים לאישור")
                for _, row in pending.iterrows():
                    c1, c2 = st.columns([3, 1])
                    c1.warning(f"{row['Full Name']} (דירה {row['Apt']})")
                    if c2.button("אשר", key=f"u_ok_{row['Phone']}"):
                        update_status_safe("Users", "Phone", str(row['Phone']).replace("'",""), 6, STATUS_ACTIVE)
                        st.rerun()
            st.divider()
            with st.expander("כל המשתמשים"):
                st.dataframe(users)

        with tab_books:
            books = get_data("Bookings")
            pending_b = books[books['Status'] == STATUS_PENDING]
            if not pending_b.empty:
                st.subheader("בקשות שיריון ממתינות")
                for _, row in pending_b.iterrows():
                    with st.container(border=True):
                        st.write(f"**{row['Date']}** | {row['Name']} (דירה {row['Apt']}) | {row['Start Time']}-{row['End Time']}")
                        c1, c2 = st.columns(2)
                        if c1.button("✅ אשר", key=f"b_ok_{row['Booking ID']}"):
                            update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_APPROVED)
                            send_telegram(f"✅ השיריון של {row['Name']} אושר!")
                            st.rerun()
                        if c2.button("❌ דחה", key=f"b_no_{row['Booking ID']}"):
                            update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_REJECTED)
                            st.rerun()
            else:
                st.success("אין בקשות שיריון חדשות")