import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ==========================================
# CẤU HÌNH DATABASE
# ==========================================
DB_CONFIG = {
    "dbname": "BaoCao", # Thay tên DB của bạn vào đây
    "user": "postgres",
    "password": "Vu123",   # Thay mật khẩu vào đây
    "host": "localhost",
    "port": "5433"
}

FILE_PATH = r"C:\Users\ADMIN\Desktop\ETL\Bảng giá von.xlsx" # Đường dẫn tới file giá vốn của bạn

def clean_code(val):
    if pd.isna(val): return ''
    if isinstance(val, (int, float)):
        try: return f"{val:.0f}" # Ép thẳng ra chuỗi số tuyệt đối (không có e+12 hay .0)
        except: pass
    s = str(val).strip().upper()
    if s.endswith('.0'): s = s[:-2]
    if s == 'NAN': return ''
    return s

def import_product_costs():
    print(f"Đang đọc file giá vốn: {FILE_PATH}")
    try:
        if FILE_PATH.lower().endswith('.csv'):
            try: df = pd.read_csv(FILE_PATH)
            except UnicodeDecodeError: df = pd.read_csv(FILE_PATH, encoding='utf-8-sig')
        else: df = pd.read_excel(FILE_PATH)
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file '{FILE_PATH}'.")
        return
        
    df.columns = df.columns.str.strip()
    df_clean = df.dropna(subset=['Mã ean']).copy()
    
    # Ép hàm làm sạch cực mạnh vào cột Mã vạch
    df_clean['Mã ean'] = df_clean['Mã ean'].apply(clean_code)
    
    df_clean['Giá nhập VND-cont'] = df_clean['Giá nhập VND-cont'].astype(str).str.replace(',', '').str.replace(' ', '')
    df_clean['Giá nhập VND-cont'] = pd.to_numeric(df_clean['Giá nhập VND-cont'], errors='coerce').fillna(0)
    df_clean = df_clean[df_clean['Mã ean'] != '']

    cost_data = [(str(row['Mã ean']), float(row['Giá nhập VND-cont'])) for _, row in df_clean.iterrows()]

    print("Bắt đầu kết nối Database để cập nhật giá vốn...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        upsert_query = """
            INSERT INTO product_costs (barcode, cost) VALUES %s
            ON CONFLICT (barcode) DO UPDATE SET cost = EXCLUDED.cost, valid_from = CURRENT_DATE, created_at = CURRENT_TIMESTAMP
        """
        execute_values(cursor, upsert_query, cost_data)
        conn.commit()
        print(f"✅ Đã import/cập nhật thành công {len(cost_data)} mã giá vốn!")
    except Exception as e:
        conn.rollback()
        print(f"❌ LỖI DATABASE: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import_product_costs()