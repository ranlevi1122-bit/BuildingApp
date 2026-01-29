import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time, date, timedelta
import requests
import uuid
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

# --- שאר הפונקציות (get_calendar_events, add_booking וכו' - נשארות כפי שהן) ---
# [כאן יש להמשיך עם הפונקציות הקיימות שלך מהקוד שעבד קודם]

# --- 4. לוגיקה עסקית ---
# def register_user(full_name, phone, apt, role, password):
#     ws = get_worksheet("Users")
#     users = get_data("Users")
#     clean_phone = phone.strip()
    
#     if not users.empty and str(clean_phone) in users['Phone'].astype(str).values:
#         return False, "הטלפון כבר קיים במערכת"

#     hashed_pw = hash_password(password)
#     ws.append_row([full_name, f"'{clean_phone}", str(apt), role, hashed_pw, STATUS_PENDING, "user"])
#     st.cache_data.clear()
    
    send_telegram(f"🔔 *הרשמה חדשה*\nשם: {full_name}\nדירה: {apt}\nטלפון: {phone}")
    return True, "בקשת ההרשמה נשלחה למנהל המערכת לאישור."

def register_user(full_name, phone, apt, role, password):
    ws = get_worksheet("Users")
    users = get_data("Users")
    clean_phone = str(phone).strip().replace("-", "").replace(" ", "").replace("'", "")
    
    # בדיקה 1: מניעת כפל טלפונים
    if not users.empty:
        users['CleanCheck'] = users['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
        if clean_phone in users['CleanCheck'].values:
            return False, "מספר הטלפון הזה כבר רשום במערכת."

    # בדיקה 2: אישור אוטומטי - כתיבה כ-active במקום pending
    ws.append_row([full_name, f"'{clean_phone}", str(apt), role, password, STATUS_ACTIVE, "user"])
    st.cache_data.clear()
    
    send_telegram(f"✅ דייר חדש נרשם ואושר אוטומטית!\nשם: {full_name}\nדירה: {apt}")
    return True, "נרשמת בהצלחה! ניתן להתחבר כעת."



# def login_user(phone, password):
#     users = get_data("Users")
#     clean_input = str(phone).strip().replace("-", "").replace(" ", "")
    
#     if users.empty: return None
    
#     users['CleanPhone'] = users['Phone'].astype(str).str.replace("'", "").str.replace("-", "").str.replace(" ", "")
#     user_row = users[users['CleanPhone'] == clean_input]
    
#     if user_row.empty: return None
    
#     stored_hash = user_row.iloc[0]['Password']
#     if verify_password(password, stored_hash):
#         return user_row.iloc[0].to_dict()
#     return None

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
            event_title = f"דירה {current_apt}\n{row['Start Time']}-{row['End Time']}"
            
            events.append({
                "title": event_title,
                "start": f"{row['Date']}T{row['Start Time']}",
                "end": f"{row['Date']}T{row['End Time']}",
                "backgroundColor": "#FFFFFF", 
                "borderColor": chosen_dot_color, 
                "textColor": "#080808"
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

def register_user(full_name, phone, apt, role, password):
    # וודא שהשורה הזו קיימת ומחוץ להערה:
    send_telegram(f"📢 *דייר חדש נרשם!*\nשם: {full_name}\nדירה: {apt}\nנא להיכנס לאפליקציה לאשר.")
    return True, "נרשמת בהצלחה! ניתן להתחבר כעת."











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
    st.title("🏡 מערכת לניהול חדר דיירים (v2)")
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
        st.button("שלח בקשה להרשמה", on_click=handle_registration)
        

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
    
    st.sidebar.title(f"שלום, {user['Full Name']}")
    
    # תפריט מותאם לפי תפקיד
    # --- מערכת התראות לאדמין ב-Sidebar ---
    if is_admin:
        # שליפת נתונים עדכניים
        u_df = get_data("Users")
        b_df = get_data("Bookings")
        
        # ספירת בקשות שממתינות לטיפול
        # משתמשים בסטטוס pending ובקשות עריכה בסטטוס edit_pending
        pend_u_count = len(u_df[u_df['Status'] == STATUS_PENDING]) if not u_df.empty else 0
        pend_b_count = len(b_df[b_df['Status'] == STATUS_EDIT_PENDING]) if not b_df.empty else 0
        
        total_alerts = pend_u_count + pend_b_count

        if total_alerts > 0:
            st.sidebar.error(f"🔔 יש לך {total_alerts} בקשות חדשות לטיפול!")
            # התראה קופצת למנהל ברגע הכניסה
            st.toast(f"מנהל, יש {total_alerts} בקשות שממתינות לך", icon="📩")
        
        # עדכון שמות התפריט עם בועיות מספר (Badges)
        u_label = f"ניהול - משתמשים {'🔴 ' + str(pend_u_count) if pend_u_count > 0 else ''}"
        b_label = f"ניהול - בקשות {'🔴 ' + str(pend_b_count) if pend_b_count > 0 else ''}"
        
        menu_opts = ["לוח שנה ושיריון", "השיריונים שלי", b_label, u_label, "ניהול - מתקדם"]
    else:
        menu_opts = ["לוח שנה ושיריון", "השיריונים שלי"]

    menu = st.sidebar.radio("תפריט", menu_opts)

    st.sidebar.markdown("---")

    if st.sidebar.button("התנתק"):
        # 1. מחיקת העוגיה
        cookie_manager.delete("logged_user_phone")
        # 2. איפוס ה-State
        st.session_state.user = None
        st.session_state.logout_clicked = True
        # 3. ניקוי מטמון וספירה לאחור
        st.cache_data.clear()
        p = st.sidebar.empty()
        for i in range(10, 0, -1):
            p.warning(f"מתנתק בבטחה... {i}")
            tm.sleep(1)
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption(f"© {datetime.now().year} כל הזכויות שמורות - רן לוי מוביל ועד הבית והאדמין")
    st.sidebar.caption("פותח עבור בניין שדרות לכיש 129 🏡")

    # --- 1. לוח שנה ושיריון (ללא שינוי מהותי) ---
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
                        # שימוש בפונקציה המעודכנת
                        ok, msg = add_booking(user, d, s, e, is_maintenance=False)
                        if ok:
                            st.toast("השיריון בוצע ואושר אוטומטית! 🎉", icon='📅')
                            tm.sleep(1) # נותן למשתמש זמן לראות את הבלון
                            st.rerun()
                        else: st.error(msg)
            
            # מקרא צבעים קטן
            st.info("💡 ירוק = שיריון רגיל | צהוב = חג | שחור/אפור = חסום")

        with col_calendar:
            # הגדרות לוח שנה
            calendar_opts = {
                "headerToolbar": {"left": "", "center": "title", "right": "prev,next"},
                "initialView": "dayGridMonth",
                "locale": "he", "direction": "rtl",
                "height": "auto", "contentHeight": "auto", "aspectRatio": 1.2,
                "displayEventTime": False,
            }
            custom_css = """
        .fc-event-time {
            display: none !important;
        }
        .fc-event-title {
            display: block !important;
            white-space: pre-line !important;
            text-align: center !important;
            line-height: 1.2 !important;
            font-size: 0.85em !important;
        }
        .fc-daygrid-event {
            display: block !important;
            padding: 2px !important;
        }
    """
            calendar(events=get_calendar_events(), options=calendar_opts, custom_css=custom_css)
            
# --- 2. השיריונים שלי (עם עריכה וביטול) ---
    elif menu == "השיריונים שלי":
        st.header(f"היסטוריית דירה {user.get('Apt', '?')}")
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
                        with st.container(border=True):
                            c1, c2, c3 = st.columns([3, 2, 2])
                            
                            # פרטי השיריון
                            if row['Status'] == STATUS_PENDING:
                                status_icon = "⏳ ממתין"
                            elif row['Status'] == STATUS_EDIT_PENDING:
                                status_icon = "📝 בעריכה"
                            else:
                                status_icon = "✅ מאושר"
                                
                            c1.write(f"**{row['Date']}** | {row['Start Time']}-{row['End Time']}")
                            c1.caption(f"{status_icon} | הוזמן ע\"י: {row['Name']}")
                            
                            # חישוב האם השיריון עתידי
                            try:
                                booking_datetime = datetime.strptime(f"{row['Date']} {row['Start Time']}", "%Y-%m-%d %H:%M")
                                is_future = booking_datetime > datetime.now()
                            except: is_future = False

                            if is_future:
                                c_edit, c_cancel = st.columns([1, 5])
                                
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
                                                
                                                if st.form_submit_button("עדכן"):
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
    elif "ניהול - בקשות" in menu and is_admin:
        st.header("ניהול בקשות")
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
                
                with st.container(border=True):
                    st.write(f"👤 **{row['Name']}** (דירה {row['Apt']}) מבקש לשנות:")
                    c_old, c_arrow, c_new = st.columns([2, 1, 2])
                    
                    if not orig_row.empty:
                        orig = orig_row.iloc[0]
                        c_old.error(f"מבוטל:\n{orig['Date']}\n{orig['Start Time']}-{orig['End Time']}")
                    else:
                        c_old.write("שיריון מקורי לא נמצא")
                        
                    c_arrow.markdown("<h2 style='text-align: center;'>⬅️</h2>", unsafe_allow_html=True)
                    c_new.success(f"חדש:\n{row['Date']}\n{row['Start Time']}-{row['End Time']}")
                    
                    b1, b2 = st.columns(2)
                    if b1.button("✅ אשר שינוי", key=f"app_ed_{row['Booking ID']}"):
                        ok, msg = approve_edit_request(row['Booking ID'], orig_id)
                        if ok: 
                            send_telegram(f"✅ בקשת השינוי של {row['Name']} אושרה!")
                            st.toast("בקשת השינוי אושרה!")
                            tm.sleep(0.5)
                            st.cache_data.clear() # ניקוי זיכרון כדי שהרשימה תתעדכן
                            st.rerun()
                            
                    if b2.button("❌ דחה שינוי", key=f"rej_ed_{row['Booking ID']}"):
                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_REJECTED):
                            st.toast("השינוי נדחה")
                            tm.sleep(0.5)
                            st.cache_data.clear()
                            st.rerun()
            st.divider()

        # --- ב. בקשות שיריון רגילות (חדשות) ---
        if not pending_new.empty:
            st.subheader("📅 בקשות שיריון חדשות")
            for _, row in pending_new.iterrows():
                with st.container(border=True):
                    st.write(f"**{row['Date']}** | {row['Name']} (דירה {row['Apt']})")
                    st.write(f"⏰ {row['Start Time']} - {row['End Time']}")
                    c1, c2 = st.columns(2)
                    
                    if c1.button("✅ אשר", key=f"adm_ok_{row['Booking ID']}"):
                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_APPROVED):
                            send_telegram(f"✅ השיריון של {row['Name']} אושר!")
                            st.toast("השיריון אושר בהצלחה!")
                            tm.sleep(0.5)
                            st.cache_data.clear()
                            st.rerun()
                            
                    if c2.button("❌ דחה", key=f"adm_no_{row['Booking ID']}"):
                        if update_status_safe("Bookings", "Booking ID", row['Booking ID'], 7, STATUS_REJECTED):
                            st.toast("הבקשה נדחתה")
                            tm.sleep(0.5)
                            st.cache_data.clear()
                            st.rerun()
        
        if pending_new.empty and pending_edit.empty:
            st.success("אין בקשות ממתינות לאישור 🎉")

    # --- 4. ניהול משתמשים (כולל אישור מהיר) ---
    elif "ניהול - משתמשים" in menu and is_admin:
        st.header("ניהול משתמשים")
        users = get_data("Users")
        
        pending = users[users['Status'] == STATUS_PENDING]
        if not pending.empty:
            st.subheader("🔔 ממתינים לאישור כניסה")
            for _, row in pending.iterrows():
                with st.container(border=True):
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
                    if st.form_submit_button("💾 שמור שינויים"):
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
    elif menu == "ניהול - מתקדם" and is_admin:
        st.header("🛠️ כלים מתקדמים")
        
        tab_block, tab_stats = st.tabs(["⛔ חסימת תאריכים", "📊 סטטיסטיקות"])
        
        # --- טאב חסימה ---
        with tab_block:
            st.write("כאן ניתן לחסום תאריכים לשיפוצים או תחזוקה.")
            with st.form("block_date_form"):
                b_date = st.date_input("תאריך לחסימה")
                b_start = st.time_input("התחלה", time(0,0))
                b_end = st.time_input("סיום", time(23,59))
                
                if st.form_submit_button("חסום זמן זה"):
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
                    st.bar_chart(day_stats.set_index('יום'), color="#43b249")
                
                with c2:
                    st.markdown("#### 🏢 דירוג שימוש לפי דירה")
                    # שימוש בגרף אופקי (Horizontal) - נראה הרבה יותר טוב לשמות/מספרי דירה
                    st.bar_chart(apt_stats.set_index('דירה'), horizontal=True, color="#3E3080")

                # 4. תוספת הנדסית: טבלת הצרכנים הכבדים (Pareto)
                with st.expander("👁️ צפה בנתוני גלם ופילוח אחוזי"):
                    st.write("פילוח שימוש יחסי לפי דירות:")
                    apt_stats['אחוז מהכלל'] = (apt_stats['הזמנות'] / total_bookings * 100).round(1).astype(str) + '%'
                    st.dataframe(apt_stats.sort_values(by='הזמנות', ascending=False), use_container_width=True)

            else:
                st.info("עדיין אין מספיק נתונים מאושרים להצגת סטטיסטיקה.")
