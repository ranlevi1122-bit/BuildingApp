import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time, date, timedelta
import requests
import uuid
import secrets as pysecrets
import holidays
from streamlit_calendar import calendar
import extra_streamlit_components as stx
import time as tm

# --- פונקציה לטעינת ה-CSS ---
def load_css(file_name):
    try:
        with open(file_name, encoding='utf-8') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except: pass

# --- רכיבי תצוגה משותפים ---
def fmt_date(date_str):
    try: return datetime.strptime(str(date_str), DATE_FMT).strftime('%d/%m/%Y')
    except: return str(date_str)

def fmt_range(start, end):
    # התווים \u2066...\u2069 שומרים על "17:00–19:00" בסדר הנכון בתוך טקסט עברי
    return f"\u2066{start}–{end}\u2069"

def page_header(title, subtitle=None):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f'<div class="page-head"><h1>{title}</h1>{sub}</div>', unsafe_allow_html=True)

# פלטת צבעים רכה - כל דירה מקבלת צבע קבוע משלה (לפי מספר הדירה)
APT_PALETTE = [
    ("#DCEFE5", "#0F766E"), ("#E3EBFA", "#3559A8"), ("#FBE6DA", "#B4572A"),
    ("#EFE3F7", "#7A45A8"), ("#DDF0F2", "#1F7A85"), ("#F8E1E6", "#A83A55"),
    ("#F3E1E1", "#8B3A3A"), ("#E2E6EE", "#4A5568"),
]
MAINTENANCE_COLORS = ("#E6E3DD", "#6B6458")
# זהב - שמור לחגים בלבד. אין להשתמש בצבע הזה לשום דבר אחר באפליקציה (סטטוסים, דירות, אזהרות)
HOLIDAY_COLORS = ("#FDF3D3", "#C8960C")
HOLIDAY_TEXT = "#6B4E00"
# ימי זיכרון וצומות אינם חגים - לא מסמנים אותם
NOT_HOLIDAYS = ("יום הזיכרון", "השואה", "תענית", "צום", "תשעה באב")

@st.cache_data(ttl=86400)
def get_jewish_holidays(year):
    """{תאריך: שם החג בעברית} - חגים יהודיים בישראל, כולל חנוכה, פורים ול"ג בעומר."""
    try:
        il = holidays.IL(years=[year], categories=holidays.IL.supported_categories, language="he")
    except Exception:
        return {}
    return {d: n for d, n in il.items() if not any(x in n for x in NOT_HOLIDAYS)}

def holiday_on(day):
    return get_jewish_holidays(day.year).get(day)

def holiday_spans(days):
    """מאחד ימים רצופים עם אותו חג: [(מיקום ראשון, אורך, שם או None)] לפי סדר הימים."""
    spans, i = [], 0
    while i < len(days):
        name = holiday_on(days[i])
        j = i
        while name and j + 1 < len(days) and days[j + 1] == days[j] + timedelta(days=1) and holiday_on(days[j + 1]) == name:
            j += 1
        spans.append((i, j - i + 1, name))
        i = j + 1
    return spans

def apt_colors(apt):
    apt = str(apt).strip()
    if apt in ("0", ""):
        return MAINTENANCE_COLORS
    try:
        return APT_PALETTE[int(apt) % len(APT_PALETTE)]
    except ValueError:
        return APT_PALETTE[-1]

# עיצוב לוח השנה - הלוח רץ בתוך iframe ולכן ה-CSS שלו מוזרק כאן ולא דרך style.css
CALENDAR_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Heebo:wght@400;500;600;700&display=swap');
html, body { background: transparent; margin: 0; }
.fc { font-family: 'Heebo', 'Segoe UI', Arial, sans-serif; color: #13302E; background: #FFFFFF;
      border-radius: 14px; --fc-border-color: #E2ECEA;
      --fc-today-bg-color: #CCEAE5; --fc-neutral-bg-color: #F5F8F7; }
.fc .fc-toolbar.fc-header-toolbar { margin: 0 !important; padding: 12px 12px 10px; }
.fc .fc-toolbar-title { font-size: 1.25rem !important; font-weight: 700; }
.fc .fc-button { background: #FFFFFF !important; border: 1px solid #D5E6E3 !important;
      color: #13302E !important; box-shadow: none !important; padding: 4px 10px !important;
      border-radius: 10px !important; }
.fc .fc-button:hover { border-color: #0F766E !important; color: #0F766E !important; }
.fc .fc-col-header-cell-cushion { white-space: nowrap; font-size: 0.8rem; font-weight: 600;
      color: #5B7471; text-decoration: none; padding: 6px 2px; }
.fc .fc-daygrid-day-number { font-size: 0.85rem; color: #5B7471; text-decoration: none; padding: 6px 8px; }
.fc .fc-day-today .fc-daygrid-day-number { color: #0F766E; font-weight: 700; }
.fc .fc-day-other .fc-daygrid-day-number { opacity: 0.45; }
.fc .fc-scrollgrid { border-radius: 10px; overflow: hidden; }
.fc-event-time { display: none !important; }
.fc .fc-daygrid-event { display: block !important; margin: 2px 3px !important; padding: 3px 4px !important;
      border-radius: 6px !important; border-width: 0 3px 0 0 !important; }
.fc .fc-bg-event.holiday-day { opacity: 1; }
.fc .fc-daygrid-event.holiday { font-weight: 700; }
.fc-event-title { display: block !important; white-space: pre-line !important; text-align: center !important;
      line-height: 1.25 !important; font-size: 0.78em !important; font-weight: 600; }
@media (max-width: 640px) { .fc .fc-toolbar.fc-header-toolbar { padding: 8px 6px; } .fc-event-title { font-size: 0.68em !important; } }
"""

# --- הגדרות ---
SHEET_ID = '1Uf_bLdIKz8aJAc1BV1OZvQwNP5Rzn4LqnQSuhL9htjg' 
DATE_FMT = '%Y-%m-%d'
TIME_FMT = '%H:%M'
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"
STATUS_ACTIVE = "active"
STATUS_EDIT_PENDING = "pending_edit"

# @st.cache_resource
def get_cookie_manager():
    return stx.CookieManager(key="auth_cookie_v4")

cookie_manager = get_cookie_manager()

# --- אבטחה (השוואת טקסט רגיל) ---
def verify_password(input_pass, stored_pass):
    # הפיכה לסטרינג מונעת את השגיאה שקיבלת (AttributeError על encode)
    return str(input_pass).strip() == str(stored_pass).strip()

# --- חיבור לגוגל שיטס ---
@st.cache_resource
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            # הזרקת התיקון: מבטיח שהמפתח הפרטי יפורמט נכון ב-Streamlit Cloud
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"שגיאה בחיבור לגוגל: {e}")
        st.stop()


# --- פונקציה לשליחת הודעות לטלגרם ---
def send_telegram(message):
    try:
        # בדיקה שהמפתחות קיימים ב-Secrets של Streamlit
        if "general" in st.secrets:
            token = st.secrets["general"]["telegram_token"]
            chat_id = st.secrets["general"]["telegram_chat_id"]
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            # שליחה עם Timeout כדי שהאפליקציה לא תיתקע אם אין אינטרנט
            requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=5)
    except Exception: 
        pass # מונע קריסה של כל האפליקציה אם יש תקלה בטלגרם

# --- טלגרם אישי לכל דייר ---
# כל דייר מחבר את הטלגרם שלו פעם אחת (מסך "השיריונים שלי"), וה-chat id נשמר בעמודה TelegramChatID בגיליון Users.
# מרגע זה, אישור/דחייה של בקשה ע"י ועד הבית שולחים לו הודעה אישית.
TG_COL = "TelegramChatID"

def _tg_token():
    try:
        return st.secrets["general"]["telegram_token"]
    except Exception:
        return None

def tg_api(method, **params):
    """קריאה ל-Bot API. מחזירה את result, או None בכל תקלה (לעולם לא מפילה את האפליקציה)."""
    token = _tg_token()
    if not token:
        return None
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/{method}", json=params, timeout=8)
        data = r.json()
        return data.get("result") if data.get("ok") else None
    except Exception:
        return None

@st.cache_data(ttl=3600)
def get_bot_username():
    me = tg_api("getMe")
    return me.get("username") if me else None

def send_telegram_to(chat_id, message):
    if chat_id and str(chat_id).strip():
        return tg_api("sendMessage", chat_id=str(chat_id).strip(), text=message) is not None
    return False

def clean_phone(p):
    return str(p).replace("'", "").replace("-", "").replace(" ", "").strip()

def get_user_chat_id(phone):
    users = get_data("Users")
    if users.empty or TG_COL not in users.columns:
        return None
    match = users[users['Phone'].map(clean_phone) == clean_phone(phone)]
    if match.empty:
        return None
    return str(match.iloc[0][TG_COL]).strip() or None

def notify_resident(phone, message):
    """שולח הודעה אישית לדייר שחיבר טלגרם. מחזיר True אם נשלחה."""
    try:
        return send_telegram_to(get_user_chat_id(phone), message)
    except Exception:
        return False

def find_telegram_start(link_token):
    """מחפש '/start <token>' שהדייר שלח לבוט, ומחזיר את ה-chat id שלו."""
    # offset=-100: 100 העדכונים האחרונים בלבד, כך שהודעה חדשה תמיד נמצאת
    updates = tg_api("getUpdates", offset=-100, allowed_updates=["message"]) or []
    for u in reversed(updates):
        msg = u.get("message") or {}
        if str(msg.get("text", "")).strip() == f"/start {link_token}":
            return str(msg["chat"]["id"])
    return None

def save_user_chat_id(phone, chat_id):
    ws = get_worksheet("Users")
    headers = [str(h).strip() for h in ws.row_values(1)]
    if TG_COL in headers:
        col = headers.index(TG_COL) + 1
    else:
        col = len(headers) + 1
        ws.update_cell(1, col, TG_COL)  # יצירת העמודה בפעם הראשונה
    cell = ws.find(f"'{phone}")
    if not cell:
        cell = ws.find(phone)
    if not cell:
        return False
    ws.update_cell(cell.row, col, str(chat_id))
    st.cache_data.clear()
    return True

def get_worksheet(name):
    client = get_gspread_client()
    sh = client.open_by_key(SHEET_ID)
    return sh.worksheet(name)


@st.cache_data(ttl=300) # TTL=300 אומר שגוגל ייקרא רק פעם ב-5 דקות
def get_data(sheet_name):
    try:
        ws = get_worksheet(sheet_name)
        # שימוש ב-values כדי להתגבר על בעיות כותרות/הקפאה
        all_values = ws.get_all_values()
        if not all_values: return pd.DataFrame()
        
        # ניקוי רווחים מהכותרות בשורה הראשונה
        headers = [str(h).strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
        return df
    except Exception as e:
        # אם יש חסימה מגוגל, האפליקציה לא תקרוס אלא תציג שגיאה ידידותית
        st.error("השרת עמוס זמנית, אנא נסה שוב בעוד דקה.")
        return pd.DataFrame()



# --- לוגיקה ---
def login_user(phone, password):
    users = get_data("Users")
    if users.empty: return None
    
    clean_phone = str(phone).strip().replace("-", "").replace(" ", "")
    users['CleanPhone'] = users['Phone'].astype(str).str.replace("'", "").str.replace(" ", "")
    
    user_row = users[users['CleanPhone'] == clean_phone]
    if user_row.empty: return None
    
    # שימוש בפונקציה החדשה ללא bcrypt
    if verify_password(password, user_row.iloc[0]['Password']):
        return user_row.iloc[0].to_dict()
    return None

def update_status_safe(sheet_name, id_col, item_id, status_col_idx, new_status):
    ws = get_worksheet(sheet_name)
    df = get_data(sheet_name)
    try:
        row_idx = df[df[id_col].astype(str) == str(item_id)].index[0] + 2
        ws.update_cell(row_idx, status_col_idx, new_status)
        st.cache_data.clear()
        return True
    except: return False


def register_user(full_name, phone, apt, role, password):
    try:
        ws = get_worksheet("Users")
        users = get_data("Users")
        clean_phone = str(phone).strip().replace("-", "").replace(" ", "").replace("'", "")
        
        # 1. בדיקת כפילות משודרגת
        if not users.empty:
            users['CleanCheck'] = users['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
            if clean_phone in users['CleanCheck'].values:
                error_msg = "⚠️ מספר הטלפון הזה כבר רשום במערכת. יש ליצור קשר עם ועד הבית לקבלת הסיסמה או לצורך איפוס המשתמש."
                st.error(error_msg) # הודעה אדומה קבועה על המסך
                st.toast(error_msg, icon="🚫") # הודעה קופצת
                
                # השהייה של 5 שניות כדי שהמשתמש יספיק לקרוא
                tm.sleep(7) 
                return False, error_msg

        # 2. הוספת השורה (כולל עמודת Is_New החדשה להתראה לאדמין)
        # שם, טלפון, דירה, סוג, סיסמה, סטטוס, תפקיד, Is_New
        new_row = [full_name, f"'{clean_phone}", str(apt), role, password, STATUS_ACTIVE, "user", "TRUE"]
        
        # שימוש ב-table_range='A1' מבטיח שהנתון יתווסף בדיוק בסוף הרשימה הקיימת
        ws.append_row(new_row, value_input_option='USER_ENTERED', table_range='A1')
        
        # 3. ניקוי מטמון ועדכון אדמין
        st.cache_data.clear()
        send_telegram(f"🔔 דייר חדש נרשם בשיטס: {full_name}\nדירה: {apt}")
        
        return True, "נרשמת בהצלחה! ניתן להתחבר כעת."
        
    except Exception as e:
        return False, f"שגיאה טכנית בתקשורת עם בסיס הנתונים: {e}"


def login_user(phone, password):
    users = get_data("Users")
    if users.empty: return None
    
    clean_input = str(phone).strip().replace("-", "").replace(" ", "")
    users['CleanPhone'] = users['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
    
    user_row = users[users['CleanPhone'] == clean_input]
    
    if user_row.empty: return None
    
    # שליפת הסיסמה מהשיטס (עמודה Password)
    stored_password = user_row.iloc[0]['Password']
    
    # כאן מתבצעת הקריאה לפונקציה שתיקנו למעלה
    if verify_password(password, stored_password):
        return user_row.iloc[0].to_dict()
    return None

def reset_new_users_notifications():
    try:
        ws = get_worksheet("Users")
        df = get_data("Users")
        if 'Is_New' in df.columns:
            # מוצאים את כל השורות שבהן Is_New הוא TRUE
            # +2 כי אינדקס מתחיל מ-0 ושורה ראשונה היא כותרת
            for idx, row in df.iterrows():
                if str(row['Is_New']).upper() == 'TRUE':
                    ws.update_cell(idx + 2, 8, "FALSE") # עמודה 8 היא Is_New
            st.cache_data.clear()
            return True
    except: return False


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

# --- פונקציה מעודכנת: הוספת שיריון עם בדיקת כפילות חכמה (Race Condition Fix) ---
def add_booking(user_data, date_obj, start, end, is_maintenance=False):
    # 1. בדיקות מקדימות
    if start >= end: return False, "שעת הסיום חייבת להיות אחרי שעת ההתחלה"
    
    date_str = date_obj.strftime(DATE_FMT)
    start_str = start.strftime(TIME_FMT)
    end_str = end.strftime(TIME_FMT)
    
    # בדיקה ראשונית בזיכרון (מהירה)
    if check_overlap(date_str, start_str, end_str):
        return False, "החדר תפוס (או ממתין לאישור) בשעות אלו"
        
    ws = get_worksheet("Bookings")
    b_id = str(uuid.uuid4())[:8]
    
    # הגדרת פרטים לפי סוג (תחזוקה או רגיל)
    name = "⛔ תחזוקה/חסום" if is_maintenance else user_data['Full Name']
    status = "approved" if is_maintenance else STATUS_PENDING
    apt = "0" if is_maintenance else str(user_data.get('Apt', '0'))
    phone = "admin" if is_maintenance else str(user_data['Phone'])

    # 2. כתיבה לגוגל שיטס
    row_data = [b_id, f"'{phone}", name, date_str, start_str, end_str, status, apt]
    ws.append_row(row_data)
    
    # 3. בדיקה חוזרת (Double Check) למניעת התנגשות בזמן אמת
    # אנחנו מנקים את הזיכרון, מושכים נתונים מחדש ובודקים אם נוצרה חפיפה כרגע
    st.cache_data.clear()
    tm.sleep(1) # נותנים לגוגל שנייה להתעדכן
    
    # בדיקה האם יש שיריון *אחר* (לא שלי) שחופף לשלי
    all_bookings = get_data("Bookings")
    current_booking = all_bookings[all_bookings['Booking ID'] == b_id]
    
    if current_booking.empty:
        return False, "שגיאה בכתיבת הנתונים"

    # בודקים שוב חפיפה מול כל השאר
    is_overlapping = False
    new_start_dt = datetime.strptime(start_str, TIME_FMT).time()
    new_end_dt = datetime.strptime(end_str, TIME_FMT).time()
    
    relevant = all_bookings[
        (all_bookings['Date'] == date_str) & 
        (all_bookings['Status'].isin([STATUS_APPROVED, STATUS_PENDING])) &
        (all_bookings['Booking ID'] != b_id) # מתעלמים מהשורה שאנחנו הרגע יצרנו
    ]
    
    for _, row in relevant.iterrows():
        ex_start = datetime.strptime(row['Start Time'], TIME_FMT).time()
        ex_end = datetime.strptime(row['End Time'], TIME_FMT).time()
        if new_start_dt < ex_end and new_end_dt > ex_start:
            is_overlapping = True
            break
    
    # 4. אם גילינו חפיפה בדיעבד - מוחקים את הבקשה שלנו!
    if is_overlapping:
        # מוצאים את השורה ומוחקים/מסמנים כדחוי
        cell = ws.find(b_id)
        if cell:
            ws.update_cell(cell.row, 7, STATUS_REJECTED) # עמודה 7 היא סטטוס
        return False, "⚠️ מצטערים, מישהו אחר הקדים אותך בשבריר שנייה. נסה שעה אחרת."

    if not is_maintenance:
        send_telegram(f"📅 *בקשה לשיריון*\nדייר: {name}\nתאריך: {date_str}\nשעות: {start_str}-{end_str}")
        return True, "הבקשה נשלחה למנהל המערכת לאישור."
    else:
        return True, "הזמן נחסם בהצלחה."

# --- פונקציה חדשה: עדכון פרטי דייר ---
# --- פונקציה מעודכנת: עדכון פרטי דייר כולל סיסמה ---
def update_user_details_admin(original_phone, new_name, new_phone, new_apt, new_type, new_password):
    ws = get_worksheet("Users")
    cell = ws.find(f"'{original_phone}") # חיפוש לפי הטלפון הישן
    if not cell:
        cell = ws.find(original_phone)
    
    if cell:
        row = cell.row
        # עדכון תאים לפי הסדר (שם, טלפון, דירה, סוג, סיסמה)
        ws.update_cell(row, 1, new_name)
        ws.update_cell(row, 2, f"'{new_phone}")
        ws.update_cell(row, 3, str(new_apt))
        ws.update_cell(row, 4, new_type)
        ws.update_cell(row, 5, new_password) # עמודה 5 היא הסיסמה
        st.cache_data.clear()
        return True
    return False

# --- פונקציה חדשה: חישוב סטטיסטיקות ---
def get_stats_data():
    df = get_data("Bookings")
    if df.empty: return None, None
    
    # סינון רק למאושרים
    df = df[df['Status'] == STATUS_APPROVED]
    
    # 1. סטטיסטיקה לפי דירה
    if 'Apt' in df.columns:
        apt_counts = df['Apt'].value_counts().reset_index()
        apt_counts.columns = ['דירה', 'הזמנות']
    else:
        apt_counts = pd.DataFrame()

    # 2. סטטיסטיקה לפי יום בשבוע
    # המרת תאריך ליום בשבוע
    df['Datetime'] = pd.to_datetime(df['Date'], format=DATE_FMT, errors='coerce')
    df = df.dropna(subset=['Datetime']) # מחיקת תאריכים לא תקינים
    
    # תרגום ימים לעברית
    days_map = {0:'שני', 1:'שלישי', 2:'רביעי', 3:'חמישי', 4:'שישי', 5:'שבת', 6:'ראשון'}
    df['Day'] = df['Datetime'].dt.dayofweek.map(days_map)
    
    day_counts = df['Day'].value_counts().reset_index()
    day_counts.columns = ['יום', 'הזמנות']
    
    return apt_counts, day_counts

def get_calendar_events():
    events = []

    # חגים: צביעת הימים עצמם (background) + פס עם שם החג שמתפרס על כל ימי החג
    hol_bg, hol_line = HOLIDAY_COLORS
    this_year = date.today().year
    all_days = [date(this_year, 1, 1) + timedelta(days=k) for k in range((date(this_year + 1, 12, 31) - date(this_year, 1, 1)).days + 1)]
    for first, length, name in holiday_spans(all_days):
        if not name:
            continue
        start = all_days[first].isoformat()
        end = (all_days[first] + timedelta(days=length)).isoformat()   # end בלעדי
        events.append({"start": start, "end": end, "allDay": True, "display": "background",
                       "backgroundColor": hol_bg, "classNames": ["holiday-day"]})
        events.append({"title": f"✡ {name}", "start": start, "end": end, "allDay": True,
                       "backgroundColor": hol_bg, "borderColor": hol_line, "textColor": HOLIDAY_TEXT,
                       "classNames": ["holiday"]})

    df = get_data("Bookings")
    if not df.empty:
        approved = df[df['Status'] == STATUS_APPROVED]
        for _, row in approved.iterrows():
            current_apt = str(row.get('Apt', '?')).strip()
            bg, line = apt_colors(current_apt)
            label = "תחזוקה" if current_apt == "0" else f"דירה {current_apt}"
            # התווים ⁦...⁩ שומרים על סדר השעות משמאל לימין בתוך טקסט עברי
            event_title = f"{label}\n⁦{row['Start Time']}–{row['End Time']}⁩"

            events.append({
                "title": event_title,
                "start": f"{row['Date']}T{row['Start Time']}",
                "end": f"{row['Date']}T{row['End Time']}",
                "display": "block",
                "backgroundColor": bg,
                "borderColor": line,
                "textColor": "#13302E"
            })
    return events

# --- פונקציה משודרגת: בדיקת חפיפה שמתעלמת משיריון ספציפי (לצורך עריכה) ---
def check_overlap_for_update(date_str, start_str, end_str, ignore_booking_id):
    bookings = get_data("Bookings")
    if bookings.empty: return False
    
    # מסננים: רק שיריונים פעילים, באותו תאריך, ולא השיריון שאנחנו עורכים כרגע!
    active = bookings[
        (bookings['Date'] == date_str) & 
        (bookings['Status'].isin([STATUS_APPROVED, STATUS_PENDING])) & 
        (bookings['Booking ID'] != ignore_booking_id) # זה החלק הקריטי
    ]
    
    if active.empty: return False
    
    new_start = datetime.strptime(start_str, TIME_FMT).time()
    new_end = datetime.strptime(end_str, TIME_FMT).time()
    
    for _, row in active.iterrows():
        ex_start = datetime.strptime(row['Start Time'], TIME_FMT).time()
        ex_end = datetime.strptime(row['End Time'], TIME_FMT).time()
        if new_start < ex_end and new_end > ex_start:
            return True
    return False

# --- פונקציה חדשה: עדכון שיריון קיים (עריכה) ---
def edit_existing_booking(booking_id, new_date, new_start, new_end):
    if new_start >= new_end: return False, "שעת הסיום חייבת להיות אחרי ההתחלה"
    
    d_str = new_date.strftime(DATE_FMT)
    s_str = new_start.strftime(TIME_FMT)
    e_str = new_end.strftime(TIME_FMT)
    
    # בדיקת חפיפה (שמתעלמת מעצמי)
    if check_overlap_for_update(d_str, s_str, e_str, booking_id):
        return False, "הזמן החדש שבחרת תפוס על ידי מישהו אחר"
    
    ws = get_worksheet("Bookings")
    cell = ws.find(booking_id)
    
    if cell:
        r = cell.row
        # עדכון תאריך, התחלה, סיום (עמודות 4, 5, 6)
        ws.update_cell(r, 4, d_str)
        ws.update_cell(r, 5, s_str)
        ws.update_cell(r, 6, e_str)
        # מחזירים לסטטוס "ממתין" אחרי עריכה? לשיקולך. כאן השארתי את הסטטוס המקורי או שאפשר לשנות.
        # ws.update_cell(r, 7, STATUS_PENDING) 
        st.cache_data.clear()
        return True, "השיריון עודכן בהצלחה!"
    return False, "שיריון לא נמצא"

# --- פונקציה חדשה: מחיקת משתמש וכל השיריונים שלו ---
def delete_user_fully_admin(phone_to_delete):
    try:
        # 1. מחיקת המשתמש
        ws_users = get_worksheet("Users")
        
        # חיפוש תא הטלפון (עם ובלי גרש)
        cell = ws_users.find(f"'{phone_to_delete}")
        if not cell: cell = ws_users.find(phone_to_delete)
        
        if cell:
            ws_users.delete_rows(cell.row)
        else:
            return False, "משתמש לא נמצא"

        # 2. מחיקת כל השיריונים של המשתמש
        ws_books = get_worksheet("Bookings")
        # אנו מוצאים את כל התאים שמכילים את הטלפון הזה
        # הערה: זה עלול לקחת זמן אם יש המון שיריונים. 
        # כדי לא להסתבך עם אינדקסים שזזים, נמחק אחד אחד בלולאה עד שאין יותר
        
        while True:
            # מחפשים מחדש בכל איטרציה כי השורות זזו
            try:
                b_cell = ws_books.find(f"'{phone_to_delete}")
                if not b_cell: b_cell = ws_books.find(phone_to_delete)
                
                if b_cell:
                    ws_books.delete_rows(b_cell.row)
                    tm.sleep(0.5) # השהייה למנוע עומס על ה-API
                else:
                    break # לא נמצאו עוד שיריונים
            except:
                break
        
        st.cache_data.clear()
        return True, "המשתמש וכל השיריונים שלו נמחקו בהצלחה"
        
    except Exception as e:
        return False, f"שגיאה במחיקה: {str(e)}"
    
# --- הגדרה קבועה לסטטוס חדש ---
STATUS_EDIT_PENDING = "pending_edit"

# --- פונקציה: דייר מבקש שינוי (יוצרת בקשה חדשה המקושרת לישנה) ---
def request_edit_booking(user_data, original_booking_id, new_date, new_start, new_end):
    # 1. בדיקות תקינות
    if new_start >= new_end: return False, "שעת הסיום חייבת להיות אחרי ההתחלה"
    
    d_str = new_date.strftime(DATE_FMT)
    s_str = new_start.strftime(TIME_FMT)
    e_str = new_end.strftime(TIME_FMT)
    
    # 2. בדיקת חפיפה (אנחנו בודקים אם *הזמן החדש* פנוי)
    # שימו לב: אנחנו לא מתעלמים מהשיריון המקורי כי הוא בזמן אחר, 
    # אבל אנחנו כן צריכים לוודא שהזמן החדש פנוי.
    if check_overlap(d_str, s_str, e_str):
        return False, "הזמן החדש שבחרת תפוס"

    # 3. יצירת רשומה חדשה בסטטוס "ממתין לעריכה"
    ws = get_worksheet("Bookings")
    new_id = str(uuid.uuid4())[:8]
    
    # מבנה השורה: ID, Phone, Name, Date, Start, End, Status, Apt, LinkedID
    # LinkedID הוא המזהה של השיריון הישן שאותו אנחנו רוצים להחליף
    row_data = [
        new_id, 
        f"'{user_data['Phone']}", 
        user_data['Full Name'], 
        d_str, 
        s_str, 
        e_str, 
        STATUS_EDIT_PENDING,     # סטטוס מיוחד
        str(user_data.get('Apt', '0')),
        original_booking_id      # הקישור לשיריון המקורי
    ]
    
    ws.append_row(row_data)
    st.cache_data.clear()
    
    send_telegram(f"✏️ *בקשת עריכה*\nדייר: {user_data['Full Name']}\nרוצה לשנות לתאריך: {d_str}\nשעות: {s_str}-{e_str}")
    return True, "בקשת השינוי נשלחה לאישור המנהל."

# --- פונקציה: אדמין מאשר שינוי (מחליף בין הישן לחדש) ---
def approve_edit_request(new_booking_id, original_booking_id):
    ws = get_worksheet("Bookings")
    
    # 1. מוצאים את השורות
    cell_new = ws.find(new_booking_id)
    cell_old = ws.find(original_booking_id)
    
    if cell_new and cell_old:
        # 2. מאשרים את החדש
        ws.update_cell(cell_new.row, 7, STATUS_APPROVED)
        
        # 3. מבטלים את הישן (סטטוס "הוחלף")
        ws.update_cell(cell_old.row, 7, "replaced")
        
        st.cache_data.clear()
        return True, "השינוי בוצע בהצלחה"
    
    return False, "שגיאה במציאת השיריונים"



# --- מסך שיריון: רצועת ימים ושעות פנויות ---
OPEN_HOUR, CLOSE_HOUR = 8, 24   # שעות הפעילות של החדר (שעה אחרונה: 23:00-24:00). שנו כאן לפי הצורך
DAY_NAMES = ['שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת', 'ראשון']   # לפי date.weekday()
DAY_SHORT = ['ב׳', 'ג׳', 'ד׳', 'ה׳', 'ו׳', 'ש׳', 'א׳']

def slot_bounds(h):
    end = time(23, 59) if h + 1 >= 24 else time(h + 1, 0)
    return time(h, 0), end

def fmt_hour_range(a, b):
    """טווח שעות לתצוגה: משבצות a עד b כולל"""
    return fmt_range(f"{a:02d}:00", f"{(b + 1) % 24:02d}:00")

def day_occupancy(day):
    """{שעה: (דירה, סטטוס)} לכל משבצת שעה תפוסה ביום הזה (מאושר, ממתין או בקשת שינוי)."""
    taken = {}
    bookings = get_data("Bookings")
    if bookings.empty:
        return taken
    rows = bookings[(bookings['Date'] == day.strftime(DATE_FMT)) &
                    (bookings['Status'].isin([STATUS_APPROVED, STATUS_PENDING, STATUS_EDIT_PENDING]))]
    for _, r in rows.iterrows():
        try:
            s = datetime.strptime(r['Start Time'], TIME_FMT).time()
            e = datetime.strptime(r['End Time'], TIME_FMT).time()
        except ValueError:
            continue
        for h in range(OPEN_HOUR, CLOSE_HOUR):
            hs, he = slot_bounds(h)
            if s < he and e > hs:
                taken[h] = (str(r.get('Apt', '')).strip(), r['Status'])
    return taken

def pick_slot(day_str, h, taken_hours):
    """לחיצה על שעה פנויה: מתחילה בחירה, מאריכה אותה, או מתחילה מחדש."""
    sel = st.session_state.get('slot_sel')
    if not sel or sel[0] != day_str:
        st.session_state.slot_sel = (day_str, h, h)
        return
    _, a, b = sel
    if a == b == h:
        st.session_state.slot_sel = None              # לחיצה שנייה על אותה שעה מבטלת
    elif a <= h <= b:
        st.session_state.slot_sel = (day_str, h, h)   # לחיצה בתוך הטווח מתחילה מחדש
    else:
        na, nb = min(a, h), max(b, h)
        if any(x in taken_hours for x in range(na, nb + 1)):
            st.session_state.slot_sel = (day_str, h, h)
            st.toast("אי אפשר לדלג על שעה תפוסה - התחלנו בחירה חדשה", icon="↩️")
        else:
            st.session_state.slot_sel = (day_str, na, nb)

def render_booking_screen(user):
    today = date.today()
    week = st.session_state.setdefault('week_offset', 0)
    first = today + timedelta(days=7 * week)
    days = [first + timedelta(days=i) for i in range(7)]

    # ניווט בין שבועות
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center", key="week_nav"):
        if st.button("→ שבוע קודם", key="week_prev", disabled=week == 0):
            st.session_state.week_offset = week - 1
            st.rerun()
        st.markdown(f"**{fmt_range(f'{days[0]:%d/%m}', f'{days[-1]:%d/%m}')}**")
        if st.button("שבוע הבא ←", key="week_next", disabled=week >= 8):
            st.session_state.week_offset = week + 1
            st.rerun()

    # רצועת ימים
    day_str = st.segmented_control(
        "יום", [d.isoformat() for d in days], default=days[0].isoformat(), required=True,
        format_func=lambda iso: f"{DAY_SHORT[date.fromisoformat(iso).weekday()]} {date.fromisoformat(iso).day}",
        key=f"day_pick_{week}", label_visibility="collapsed", width="stretch",
    )
    day = date.fromisoformat(day_str)

    # חגים בשבוע המוצג: סימון זהב על הימים + פס עם שם החג שמתפרס על ימי החג
    spans = holiday_spans(days)
    if any(name for _, _, name in spans):
        hol_bg, hol_line = HOLIDAY_COLORS
        sel = f".st-key-day_pick_{week} [data-testid='stButtonGroup'] button"
        css = "".join(
            f"{sel}:nth-of-type({k + 1}) {{ box-shadow: inset 0 -4px 0 {hol_line}; }}"
            f"{sel}:nth-of-type({k + 1})[aria-checked='false'], {sel}:nth-of-type({k + 1})[aria-pressed='false'] {{ background: {hol_bg}; }}"
            for first, length, name in spans if name for k in range(first, first + length)
        )
        cells = "".join(
            f'<div class="hol-band" style="grid-column: span {length}">✡ {name}</div>' if name
            else f'<div style="grid-column: span {length}"></div>'
            for _, length, name in spans
        )
        st.markdown(f'<style>{css}</style><div class="hol-row">{cells}</div>', unsafe_allow_html=True)

    with st.container(border=True, key="card_slots"):
        hol = holiday_on(day)
        title = f"{'היום · ' if day == today else ''}יום {DAY_NAMES[day.weekday()]} {day:%d/%m}"
        st.markdown(f"#### {title}")
        if hol:
            st.markdown(f'<div style="text-align:center"><span class="pill pill-holiday">✡ {hol}</span></div>', unsafe_allow_html=True)

        taken = day_occupancy(day)
        now = datetime.now()
        sel = st.session_state.get('slot_sel')
        if sel and sel[0] != day_str:
            sel = None
        free_count = 0

        with st.container(horizontal=True, wrap=True, gap="small", key="slot_grid"):
            for h in range(OPEN_HOUR, CLOSE_HOUR):
                label = fmt_hour_range(h, h)
                key = f"slot_{day_str}_{h}"
                if h in taken:
                    apt, status = taken[h]
                    who = "תחזוקה" if apt == "0" else ("ממתין" if status != STATUS_APPROVED else f"דירה {apt}")
                    st.button(f"{label} · {who}", key=key, disabled=True)
                elif datetime.combine(day, slot_bounds(h)[0]) <= now:
                    st.button(label, key=key, disabled=True)
                else:
                    free_count += 1
                    chosen = bool(sel) and sel[1] <= h <= sel[2]
                    st.button(label, key=key, type="primary" if chosen else "secondary",
                              on_click=pick_slot, args=(day_str, h, tuple(taken)))

        if sel:
            start, _ = slot_bounds(sel[1])
            _, end = slot_bounds(sel[2])
            if st.button(f"בקשת שיריון · {fmt_hour_range(sel[1], sel[2])}", type="primary",
                         use_container_width=True, key="book_cta"):
                ok, msg = add_booking(user, day, start, end, is_maintenance=False)
                if ok:
                    st.session_state.slot_sel = None
                    st.session_state.booking_flash = f"הבקשה ליום {DAY_NAMES[day.weekday()]} {day:%d/%m}, {fmt_hour_range(sel[1], sel[2])}, נשלחה לוועד הבית"
                    st.rerun()
                else:
                    st.error(msg)
        elif free_count == 0:
            st.info("אין שעות פנויות ביום הזה - נסו יום אחר")
        else:
            st.caption("לחצו על שעה פנויה כדי להתחיל, ועל שעה נוספת כדי להאריך את הטווח")

    flash = st.session_state.pop('booking_flash', None)
    if flash:
        st.success(flash)
        if get_user_chat_id(user.get('Phone', '')):
            st.caption("🔔 נשלח לכם עדכון בטלגרם ברגע שהבקשה תאושר")
        elif _tg_token():
            st.caption("רוצים הודעה בטלגרם כשהבקשה מאושרת? חברו טלגרם במסך \"השיריונים שלי\"")

    # תצוגת חודש (לבקשה)
    if st.toggle("הצגת לוח חודשי", key="show_month"):
        st.markdown(
            f'<div class="legend">'
            f'<span><i style="background:{APT_PALETTE[0][1]}"></i><i style="background:{APT_PALETTE[1][1]}"></i><i style="background:{APT_PALETTE[2][1]}"></i>שיריון - צבע לכל דירה</span>'
            f'<span><i style="background:{HOLIDAY_COLORS[1]}"></i>חג</span>'
            f'<span><i style="background:{MAINTENANCE_COLORS[1]}"></i>תחזוקה / חסום</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        calendar_opts = {
            "headerToolbar": {"left": "", "center": "title", "right": "prev,next"},
            "initialView": "dayGridMonth",
            "locale": "he", "direction": "rtl",
            "height": "auto", "contentHeight": "auto", "aspectRatio": 1.2,
            "displayEventTime": False,
            "fixedWeekCount": False,
        }
        calendar(events=get_calendar_events(), options=calendar_opts, custom_css=CALENDAR_CSS, key="main_calendar")

def render_telegram_card(user):
    phone = clean_phone(user.get('Phone', ''))
    if not _tg_token() or not phone or phone == 'admin':
        return
    with st.container(border=True, key="card_telegram"):
        if get_user_chat_id(phone):
            st.markdown("**🔔 התראות בטלגרם פעילות**")
            st.caption("תקבלו הודעה בטלגרם ברגע שוועד הבית מאשר או דוחה בקשה שלכם.")
            return
        st.markdown("**🔔 קבלו הודעה בטלגרם כשהבקשה מאושרת**")
        bot = get_bot_username()
        if not bot:
            st.caption("החיבור לטלגרם לא זמין כרגע.")
            return
        token = st.session_state.setdefault('tg_link_token', pysecrets.token_urlsafe(9))
        st.caption("פתחו את הבוט ולחצו Start, ואז חזרו לכאן ולחצו \"בדיקת חיבור\"")
        with st.container(horizontal=True, gap="small", key="tg_actions"):
            st.link_button("פתיחת הבוט בטלגרם", f"https://t.me/{bot}?start={token}", type="primary")
            check = st.button("בדיקת חיבור", key="tg_check")
        if check:
            chat = find_telegram_start(token)
            if chat and save_user_chat_id(phone, chat):
                send_telegram_to(chat, "✅ חוברתם להתראות חדר הדיירים. נעדכן אתכם כאן כשוועד הבית מאשר בקשה.")
                st.session_state.pop('tg_link_token', None)
                st.toast("טלגרם חובר", icon="✅")
                st.rerun()
            else:
                st.warning("עוד לא קיבלנו את ה-Start מהבוט. ודאו שלחצתם Start בטלגרם ונסו שוב.")

# --- האפליקציה הראשית ---
st.set_page_config(page_title="ניהול חדר דיירים", layout="wide")

load_css("style.css")


if 'user' not in st.session_state: st.session_state.user = None

# === בדיקת עוגיות (Auto Login) עם הגנה מחיבור מחדש ===
if st.session_state.user is None:
    # בדיקה: האם הרגע לחצנו על התנתק? אם כן, דלג על בדיקת העוגיה
    if st.session_state.get('logout_clicked', False):
        st.session_state.logout_clicked = False # אפס את הדגל לפעם הבאה
    else:
        # קריאת העוגיה
        cookie_phone = cookie_manager.get(cookie="logged_user_phone")
        
        # מוודאים שהעוגיה קיימת ושיש בה תוכן אמיתי (לא ריקה)
        if cookie_phone and str(cookie_phone).strip() != "":
            users_db = get_data("Users")
            if not users_db.empty:
                # ניקוי הטלפון מהעוגיה כדי להשוות לדאטה בייס
                users_db['CleanPhone'] = users_db['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
                found_user = users_db[users_db['CleanPhone'] == str(cookie_phone)]
                
                if not found_user.empty:
                    # מצאנו משתמש תואם לעוגיה - מחברים אותו
                    st.session_state.user = found_user.iloc[0].to_dict()
                    st.rerun()

if st.session_state.user is None:
    if st.session_state.get('logout_clicked', False):
        pass 
    else:
        cookie_phone = cookie_manager.get(cookie="logged_user_phone")
        if cookie_phone and len(str(cookie_phone)) > 5:
            users_db = get_data("Users")
            if not users_db.empty:
                users_db['CleanPhone'] = users_db['Phone'].astype(str).str.replace("'", "").str.replace(" ", "")
                found_user = users_db[users_db['CleanPhone'] == str(cookie_phone)]
                if not found_user.empty:
                    u_data = found_user.iloc[0].to_dict()
                    if u_data.get('Status') == STATUS_ACTIVE:
                        st.session_state.user = u_data
                        st.rerun()

# --- מסך התחברות / הרשמה ---
if not st.session_state.user:
    _, auth_col, _ = st.columns([1, 1.4, 1])
    with auth_col:
        st.markdown(
            '<div class="brand-hero"><div class="logo">🏡</div>'
            '<h1>חדר הדיירים</h1><p>שיריון וניהול חדר הדיירים · שדרות לכיש 129</p></div>',
            unsafe_allow_html=True,
        )
        tab1, tab2 = st.tabs(["כניסה", "הרשמה"])

        with tab1:
            l_phone = st.text_input("טלפון נייד", placeholder="050-0000000")
            l_pass = st.text_input("סיסמה", type="password")
            if st.button("התחבר", type="primary", use_container_width=True):
                if l_phone == "admin" and l_pass == "admin123":
                     st.session_state.user = {'Role': 'admin', 'Full Name': 'מנהל על', 'Apt': '0', 'Phone': 'admin'}
                     st.rerun()
            
                user = login_user(l_phone, l_pass)
                if user:
                    if user['Status'] == STATUS_ACTIVE:
                        st.session_state.user = user
                    
                        # === שמירת עוגיה תקינה ===
                        clean_phone_cookie = str(l_phone).strip().replace("-", "").replace(" ", "")
                        # התיקון: שימוש ב-timedelta
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
        
            # --- 1. פונקציית Callback לטיפול בהרשמה ---
            def handle_registration():
                # שולפים את הנתונים ישירות מה-State
                name = st.session_state.reg_name
                phone = st.session_state.reg_phone
                apt = st.session_state.reg_apt
                user_type = st.session_state.reg_type
                password = st.session_state.reg_pass
            
                # בדיקת תקינות
                if name and phone and password:
                    # קריאה לפונקציית ההרשמה
                    ok, msg = register_user(name, phone, apt, user_type, password)
                
                    if ok:
                        # שמירת הודעת הצלחה בזיכרון להצגה
                        st.session_state['reg_message'] = ('success', msg)
                    
                        # איפוס השדות - מותר לעשות את זה כאן כי אנחנו בתוך Callback!
                        st.session_state.reg_name = ""
                        st.session_state.reg_phone = ""
                        st.session_state.reg_apt = 1
                        st.session_state.reg_type = "בעל דירה"
                        st.session_state.reg_pass = ""
                    else:
                        # שמירת הודעת שגיאה
                        st.session_state['reg_message'] = ('error', msg)
                else:
                    st.session_state['reg_message'] = ('error', "נא למלא את כל השדות")

            # --- 2. ציור הטופס ---
            st.text_input("שם מלא", key="reg_name")
            st.text_input("טלפון", key="reg_phone")
            st.number_input("מספר דירה", min_value=1, max_value=49, step=1, key="reg_apt")
            st.selectbox("אני...", ["בעל דירה", "שוכר"], key="reg_type")
            st.text_input("בחר סיסמה", type="password", key="reg_pass")
        
            # כפתור עם קישור לפונקציה (on_click)
            st.button("שלח בקשה להרשמה", on_click=handle_registration, type="primary", use_container_width=True)
        

            # --- 3. הצגת הודעות (אם יש) ---
            if 'reg_message' in st.session_state:
                msg_type, msg_text = st.session_state['reg_message']
                if msg_type == 'success':
                    st.success(msg_text)
                else:
                    st.error(msg_text)
            
                # מחיקת ההודעה כדי שלא תופיע שוב סתם בריענון הבא
                del st.session_state['reg_message']

# --- המערכת פנימה ---
else:
    user = st.session_state.user
    is_admin = user.get('Role') in ['admin', 'committee']
    
    full_name = str(user.get('Full Name', '')).strip() or "דייר"
    role_label = "ועד הבית" if is_admin else f"דירה {user.get('Apt', '?')}"

    # --- ספירת בקשות ממתינות (לאדמין) ---
    pend_u_count = pend_b_count = 0
    if is_admin:
        u_df = get_data("Users")
        b_df = get_data("Bookings")
        pend_u_count = len(u_df[u_df['Status'] == STATUS_PENDING]) if not u_df.empty else 0
        pend_b_count = len(b_df[b_df['Status'].isin([STATUS_PENDING, STATUS_EDIT_PENDING])]) if not b_df.empty else 0
        total_alerts = pend_u_count + pend_b_count
        # התראה קופצת פעם אחת בלבד בכל כניסה (ולא בכל לחיצה)
        if total_alerts > 0 and not st.session_state.get('admin_alert_shown'):
            st.toast(f"יש {total_alerts} בקשות שממתינות לך", icon="📩")
            st.session_state.admin_alert_shown = True

    # --- שורה עליונה: המשתמש והתנתקות ---
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center", key="top_bar"):
        st.markdown(
            f'<div class="side-user"><div class="side-avatar">{full_name[0]}</div>'
            f'<div><div class="side-name">שלום, {full_name}</div>'
            f'<div class="side-meta">{role_label}</div></div></div>',
            unsafe_allow_html=True,
        )
        if st.button("התנתק", key="logout_btn"):
            # 1. מחיקת העוגיה  2. איפוס ה-State  3. השהייה קצרה כדי שמחיקת העוגיה תגיע לדפדפן
            cookie_manager.delete("logged_user_phone")
            st.session_state.user = None
            st.session_state.logout_clicked = True
            for k in ('admin_alert_shown', 'slot_sel', 'tg_link_token', 'nav', 'last_nav', 'week_offset'):
                st.session_state.pop(k, None)
            st.cache_data.clear()
            with st.spinner("מתנתק..."):
                tm.sleep(1)
            st.rerun()

    # --- ניווט עליון (במקום תפריט צד שמוסתר בנייד) ---
    nav_labels = {"book": "📅 שיריון", "mine": "🗂️ השיריונים שלי"}
    if is_admin:
        nav_labels["requests"] = f"📥 בקשות{f' ({pend_b_count})' if pend_b_count else ''}"
        nav_labels["users"] = f"👥 דיירים{f' ({pend_u_count})' if pend_u_count else ''}"
        nav_labels["advanced"] = "🛠️ מתקדם"
    menu = st.segmented_control(
        "תפריט", list(nav_labels), format_func=nav_labels.get, default="book", required=True,
        key="nav", label_visibility="collapsed", width="stretch",
    ) or "book"

    # --- 1. שיריון: רצועת ימים ושעות פנויות ---
    if menu == "book":
        page_header("שיריון חדר הדיירים", "בחרו יום ושעות פנויות - הבקשה תישלח לאישור ועד הבית")
        render_booking_screen(user)
            
# --- 2. השיריונים שלי (עם עריכה וביטול) ---
    elif menu == "mine":
        page_header("השיריונים שלי", f"כל השיריונים של דירה {user.get('Apt', '?')}")
        render_telegram_card(user)
        df = get_data("Bookings")
        
        if not df.empty:
            user_apt = str(user.get('Apt', '')).strip()
            if 'Apt' in df.columns:
                df['Apt'] = df['Apt'].astype(str).str.strip()
                # מציג שיריונים של הדירה (מאושרים, ממתינים, או ממתינים לעריכה)
                # הוספנו את STATUS_EDIT_PENDING כדי שיראו גם בקשות עריכה של הדירה
                my_bookings = df[(df['Apt'] == user_apt) & (df['Status'].isin([STATUS_APPROVED, STATUS_PENDING, STATUS_EDIT_PENDING]))]
                
                if not my_bookings.empty:
                    # מיין לפי תאריך (הכי קרוב למעלה)
                    my_bookings = my_bookings.sort_values(by='Date', ascending=False)
                    
                    for _, row in my_bookings.iterrows():
                        with st.container(border=True, key=f"card_my_{row['Booking ID']}"):
                            c1, c2, c3 = st.columns([3, 2, 2])
                            
                            # פרטי השיריון
                            if row['Status'] == STATUS_PENDING:
                                status_pill = '<span class="pill pill-wait">⏳ ממתין לאישור</span>'
                            elif row['Status'] == STATUS_EDIT_PENDING:
                                status_pill = '<span class="pill pill-edit">📝 בקשת שינוי</span>'
                            else:
                                status_pill = '<span class="pill pill-ok">✓ מאושר</span>'

                            c1.markdown(
                                f'<div class="booking-when">{fmt_date(row["Date"])} '
                                f'<span class="booking-time">{row["Start Time"]}–{row["End Time"]}</span></div>',
                                unsafe_allow_html=True,
                            )
                            c1.caption(f"הוזמן ע\"י {row['Name']}")
                            c2.markdown(status_pill, unsafe_allow_html=True)
                            
                            # חישוב האם השיריון עתידי
                            try:
                                booking_datetime = datetime.strptime(f"{row['Date']} {row['Start Time']}", "%Y-%m-%d %H:%M")
                                is_future = booking_datetime > datetime.now()
                            except: is_future = False

                            if is_future:
                                c_edit = c_cancel = c3.container(horizontal=True, gap="small", key=f"actions_{row['Booking ID']}")  # עריכה ומחיקה באותה שורה
                                
                                # --- כפתור עריכה (הלוגיקה המתוקנת) ---
                                with c_edit:
                                    # אם השיריון כבר בסטטוס עריכה - חוסמים עריכה נוספת
                                    if row['Status'] == STATUS_EDIT_PENDING:
                                        st.caption("ממתין...")
                                    else:
                                        with st.popover("✏️"): # כפתור קטן עם עיפרון
                                            st.write("עריכת שיריון")
                                            # המרת מחרוזות לאובייקטים
                                            curr_d = datetime.strptime(row['Date'], DATE_FMT).date()
                                            curr_s = datetime.strptime(row['Start Time'], TIME_FMT).time()
                                            curr_e = datetime.strptime(row['End Time'], TIME_FMT).time()
                                            
                                            with st.form(f"edit_form_{row['Booking ID']}"):
                                                new_d = st.date_input("תאריך", value=curr_d)
                                                new_s = st.time_input("התחלה", value=curr_s)
                                                new_e = st.time_input("סיום", value=curr_e)
                                                
                                                if st.form_submit_button("עדכן", use_container_width=True):
                                                    # --- כאן התיקון שלך ---
                                                    if is_admin:
                                                        # אדמין: מעדכן מיד
                                                        ok, msg = edit_existing_booking(row['Booking ID'], new_d, new_s, new_e)
                                                    else:
                                                        # משתמש רגיל: שולח בקשה לאישור
                                                        ok, msg = request_edit_booking(user, row['Booking ID'], new_d, new_s, new_e)
                                                    
                                                    if ok:
                                                        st.success(msg)
                                                        tm.sleep(1.5)
                                                        st.rerun()
                                                    else:
                                                        st.error(msg)

                                # --- כפתור ביטול ---
                                with c_cancel:
                                    if st.button("🗑️", key=f"cncl_{row['Booking ID']}"): # כפתור קטן עם פח
                                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, "cancelled_by_user"):
                                            st.success("בוטל!")
                                            tm.sleep(1.5)
                                            st.rerun()
                            else:
                                # שיריון עבר
                                st.write("") 
                else:
                    st.info("אין שיריונים פעילים לדירה זו")
            else:
                st.error("חסרה עמודת Apt בנתונים")

# --- 3. ניהול בקשות משודרג (כולל עריכות) ---
    elif menu == "requests" and is_admin:
        page_header("ניהול בקשות", "אישור או דחייה של בקשות שיריון ושינוי מועד")
        # משיכת הנתונים (מוגן ב-TTL של 5 דקות)
        books = get_data("Bookings")
        
        # הפרדה בין בקשות חדשות לבקשות עריכה
        pending_new = books[books['Status'] == STATUS_PENDING]
        pending_edit = books[books['Status'] == STATUS_EDIT_PENDING]
        
        # --- א. בקשות עריכה/שינוי ---
        if not pending_edit.empty:
            st.subheader("✏️ בקשות לשינוי מועד")
            for _, row in pending_edit.iterrows():
                orig_id = str(row.get('LinkedID', '')).strip()
                orig_row = books[books['Booking ID'] == orig_id]
                
                with st.container(border=True, key=f"card_edit_{row['Booking ID']}"):
                    st.write(f"👤 **{row['Name']}** (דירה {row['Apt']}) מבקש לשנות:")
                    c_old, c_arrow, c_new = st.columns([2, 0.4, 2], vertical_alignment="center")
                    
                    if not orig_row.empty:
                        orig = orig_row.iloc[0]
                        c_old.error(f"**במקום:** {fmt_date(orig['Date'])} · {fmt_range(orig['Start Time'], orig['End Time'])}")
                    else:
                        c_old.write("שיריון מקורי לא נמצא")
                        
                    c_arrow.markdown("<h3 style='text-align: center; margin: 0.4rem 0;'>←</h3>", unsafe_allow_html=True)
                    c_new.success(f"**מועד חדש:** {fmt_date(row['Date'])} · {fmt_range(row['Start Time'], row['End Time'])}")
                    
                    b1, b2 = st.columns(2)
                    if b1.button("✓ אשר שינוי", key=f"app_ed_{row['Booking ID']}", type="primary", use_container_width=True):
                        ok, msg = approve_edit_request(row['Booking ID'], orig_id)
                        if ok: 
                            send_telegram(f"✅ בקשת השינוי של {row['Name']} אושרה!")
                            sent = notify_resident(row['Phone'], f"✅ שינוי המועד שלך אושר\nהמועד החדש: {fmt_date(row['Date'])} · {row['Start Time']}–{row['End Time']}")
                            st.toast("בקשת השינוי אושרה" + (" · הדייר עודכן בטלגרם" if sent else ""))
                            tm.sleep(0.5)
                            st.cache_data.clear() # ניקוי זיכרון כדי שהרשימה תתעדכן
                            st.rerun()
                            
                    if b2.button("✕ דחה שינוי", key=f"rej_ed_{row['Booking ID']}", use_container_width=True):
                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_REJECTED):
                            sent = notify_resident(row['Phone'], f"❌ בקשת שינוי המועד ל-{fmt_date(row['Date'])} · {row['Start Time']}–{row['End Time']} לא אושרה.\nהשיריון המקורי שלך נשאר בתוקף.")
                            st.toast("השינוי נדחה" + (" · הדייר עודכן בטלגרם" if sent else ""))
                            tm.sleep(0.5)
                            st.cache_data.clear()
                            st.rerun()
            st.divider()

        # --- ב. בקשות שיריון רגילות (חדשות) ---
        if not pending_new.empty:
            st.subheader("📅 בקשות שיריון חדשות")
            for _, row in pending_new.iterrows():
                with st.container(border=True, key=f"card_req_{row['Booking ID']}"):
                    st.markdown(f"**{fmt_date(row['Date'])}** · {fmt_range(row['Start Time'], row['End Time'])}")
                    st.caption(f"{row['Name']} · דירה {row['Apt']}")
                    c1, c2 = st.columns(2)
                    
                    if c1.button("✓ אשר", key=f"adm_ok_{row['Booking ID']}", type="primary", use_container_width=True):
                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_APPROVED):
                            send_telegram(f"✅ השיריון של {row['Name']} אושר!")
                            sent = notify_resident(row['Phone'], f"✅ השיריון שלך לחדר הדיירים אושר\n{fmt_date(row['Date'])} · {row['Start Time']}–{row['End Time']}")
                            st.toast("השיריון אושר" + (" · הדייר עודכן בטלגרם" if sent else ""))
                            tm.sleep(0.5)
                            st.cache_data.clear()
                            st.rerun()
                            
                    if c2.button("✕ דחה", key=f"adm_no_{row['Booking ID']}", use_container_width=True):
                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_REJECTED):
                            sent = notify_resident(row['Phone'], f"❌ בקשת השיריון שלך ל-{fmt_date(row['Date'])} · {row['Start Time']}–{row['End Time']} לא אושרה.\nאפשר לבחור מועד אחר באפליקציה.")
                            st.toast("הבקשה נדחתה" + (" · הדייר עודכן בטלגרם" if sent else ""))
                            tm.sleep(0.5)
                            st.cache_data.clear()
                            st.rerun()
        
        if pending_new.empty and pending_edit.empty:
            st.success("אין בקשות ממתינות לאישור 🎉")

    # --- 4. ניהול משתמשים (כולל אישור מהיר) ---
    elif menu == "users" and is_admin:
        page_header("ניהול משתמשים", "אישור דיירים חדשים, עריכה ומחיקה")
        users = get_data("Users")
        
        pending = users[users['Status'] == STATUS_PENDING]
        if not pending.empty:
            st.subheader("🔔 ממתינים לאישור כניסה")
            for _, row in pending.iterrows():
                with st.container(border=True, key=f"card_user_{row['Phone']}"):
                    c1, c2 = st.columns([3, 1])
                    # מנקים את מספר הטלפון מכל גרש או רווח לצורך התצוגה והחיפוש
                    display_phone = str(row['Phone']).replace("'", "").strip()
                    c1.warning(f"**{row['Full Name']}** | דירה {row['Apt']} | {display_phone}")
                    
                    if c2.button("אשר דייר", key=f"u_ok_{display_phone}"):
                        with st.spinner("מאשר משתמש..."):
                            # אנחנו שולחים את הטלפון הנקי לחיפוש
                            success = update_status_safe("Users", "Phone", display_phone, 6, STATUS_ACTIVE)
                            
                            if success:
                                st.toast(f"המשתמש {row['Full Name']} אושר!")
                                st.cache_data.clear()
                                tm.sleep(1)
                                st.rerun()
                            else:
                                # אם נכשל, ננסה שוב עם הגרש (למקרה שגוגל מחייב אותו)
                                success_with_tick = update_status_safe("Users", "Phone", f"'{display_phone}", 6, STATUS_ACTIVE)
                                if success_with_tick:
                                    st.toast(f"המשתמש {row['Full Name']} אושר!")
                                    st.cache_data.clear()
                                    tm.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("לא הצלחתי למצוא את המשתמש בגיליון. בדוק אם עמודת הסטטוס היא אכן מספר 6.")
        # if not pending.empty:
        #     st.subheader("🔔 ממתינים לאישור כניסה")
        #     for _, row in pending.iterrows():
        #         with st.container(border=True):
        #             c1, c2 = st.columns([3, 1])
        #             c1.warning(f"{row['Full Name']} | דירה {row['Apt']} | {row['Phone']}")
                    
        #             if c2.button("אשר דייר", key=f"u_ok_{row['Phone']}"):
        #                 clean_phone = str(row['Phone']).replace("'","").strip()
        #                 if update_status_safe("Users", "Phone", clean_phone, 6, STATUS_ACTIVE):
        #                     st.toast(f"המשתמש {row['Full Name']} אושר!")
        #                     tm.sleep(0.5)
        #                     st.cache_data.clear()
        #                     st.rerun()
        #     st.divider()

        # עריכה ומחיקה
        st.subheader("✏️ עריכה / מחיקת דייר")
        
        # יצירת לייבל לבחירה
        users['SelectLabel'] = users['Full Name'].astype(str) + " (" + users['Phone'].astype(str) + ")"
        user_select = st.selectbox("בחר דייר", users['SelectLabel'].tolist())
        
        if user_select:
            user_to_edit = users[users['SelectLabel'] == user_select].iloc[0]
            orig_phone = str(user_to_edit['Phone']).replace("'","")
            
            with st.form("edit_user_admin"):
                st.write(f"משתמש: **{user_to_edit['Full Name']}**")
                
                c1, c2 = st.columns(2)
                new_n = c1.text_input("שם", value=user_to_edit['Full Name'])
                new_p = c2.text_input("טלפון", value=orig_phone)
                c3, c4 = st.columns(2)
                new_a = c3.text_input("דירה", value=str(user_to_edit['Apt']))
                new_t = c4.selectbox("סוג", ["בעל דירה", "שוכר"], index=0 if user_to_edit['Type'] == "בעל דירה" else 1)
                
                new_pass = st.text_input("סיסמה", value=str(user_to_edit['Password']))
                
                col_save, col_del = st.columns([1, 1])
                
                # כפתור שמירה (ירוק)
                with col_save:
                    if st.form_submit_button("💾 שמור שינויים", use_container_width=True):
                        if update_user_details_admin(orig_phone, new_n, new_p, new_a, new_t, new_pass):
                            st.success("עודכן!")
                            tm.sleep(1)
                            st.rerun()
                        else:
                            st.error("שגיאה")

            # כפתור מחיקה (אדום - מחוץ לטופס כדי למנוע סגירה)
            st.markdown("---")
            st.write("🗑️ **אזור מסוכן**")
            with st.expander("מחיקת משתמש לצמיתות"):
                st.error("פעולה זו תמחק את המשתמש וגם את כל השיריונים העתידיים וההיסטוריים שלו!")
                if st.button("מחק את המשתמש והנתונים שלו", type="primary"):
                    ok, msg = delete_user_fully_admin(orig_phone)
                    if ok:
                        st.success(msg)
                        tm.sleep(2)
                        st.rerun()
                    else:
                        st.error(msg)

    # --- 5. ניהול מתקדם (חסימות וסטטיסטיקה) ---
    elif menu == "advanced" and is_admin:
        page_header("כלים מתקדמים", "חסימת זמנים לתחזוקה ונתוני שימוש")
        
        tab_block, tab_stats = st.tabs(["⛔ חסימת תאריכים", "📊 סטטיסטיקות"])
        
        # --- טאב חסימה ---
        with tab_block:
            st.write("כאן ניתן לחסום תאריכים לשיפוצים או תחזוקה.")
            with st.form("block_date_form"):
                b_date = st.date_input("תאריך לחסימה")
                b_start = st.time_input("התחלה", time(0,0))
                b_end = st.time_input("סיום", time(23,59))
                
                if st.form_submit_button("חסום זמן זה", use_container_width=True):
                    # קריאה ל-add_booking עם דגל מיוחד
                    ok, msg = add_booking({}, b_date, b_start, b_end, is_maintenance=True)
                    if ok: st.success("התאריך נחסם בהצלחה")
                    else: st.error(msg)
        
        # --- טאב סטטיסטיקות ---
        with tab_stats:
            st.subheader("📊 דשבורד שימוש וביצועים")
            apt_stats, day_stats = get_stats_data()
            
            if apt_stats is not None and not apt_stats.empty:
                # 1. חישוב נתונים ל-Metrics
                # שליפת נתוני גלם מה-DB כדי לחשב אחוזים
                df_all = get_data("Bookings")
                df_approved = df_all[df_all['Status'] == STATUS_APPROVED]
                
                total_bookings = len(df_approved)
                active_users = df_approved['Apt'].nunique()
                most_busy_day = day_stats.loc[day_stats['הזמנות'].idxmax(), 'יום']
                
                # 2. תצוגת מדדי מפתח (KPIs)
                col1, col2, col3 = st.columns(3)
                col1.metric("סה\"כ אירועים שאושרו", total_bookings, help="מספר השיריונים הכולל בסטטוס מאושר")
                col2.metric("דירות פעילות", f"{active_users} / 49", help="כמה דירות שונות השתמשו בחדר")
                col3.metric("שיא פעילות", most_busy_day, help="היום בשבוע בו החדר הכי מבוקש")
                
                st.divider()

                # 3. תצוגת גרפים בשתי עמודות
                c1, c2 = st.columns([1.2, 1], gap="medium") # עמודה אחת מעט רחבה יותר
                
                with c1:
                    st.markdown("#### 📅 התפלגות עומס לפי ימים")
                    # שינוי צבע לגרף ימים - ירוק מותאם למותג
                    st.bar_chart(day_stats.set_index('יום'), color="#0F766E")
                
                with c2:
                    st.markdown("#### 🏢 דירוג שימוש לפי דירה")
                    # שימוש בגרף אופקי (Horizontal) - נראה הרבה יותר טוב לשמות/מספרי דירה
                    st.bar_chart(apt_stats.set_index('דירה'), horizontal=True, color="#3559A8")

                # 4. תוספת הנדסית: טבלת הצרכנים הכבדים (Pareto)
                with st.expander("👁️ צפה בנתוני גלם ופילוח אחוזי"):
                    st.write("פילוח שימוש יחסי לפי דירות:")
                    apt_stats['אחוז מהכלל'] = (apt_stats['הזמנות'] / total_bookings * 100).round(1).astype(str) + '%'
                    st.dataframe(apt_stats.sort_values(by='הזמנות', ascending=False), use_container_width=True)

            else:
                st.info("עדיין אין מספיק נתונים מאושרים להצגת סטטיסטיקה.")

    st.divider()
    st.caption(f"© {datetime.now().year} רן לוי · ועד הבית · פותח עבור בניין שדרות לכיש 129 🏡")
