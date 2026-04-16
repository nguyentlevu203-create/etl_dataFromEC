import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG DATABASE & THƯ MỤC
# ==========================================
DB_CONFIG = {
    "dbname": "BaoCao", # ĐIỀN TÊN DATABASE
    "user": "postgres",
    "password": "Vu123",   # ĐIỀN MẬT KHẨU
    "host": "localhost",
    "port": "5433"
}

CHANNEL_NHANH_ID = 1
INPUT_DIR = "data/nhanh/orders/"
PROCESSED_DIR = "processed/nhanh/orders/"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ==========================================
# 2. HÀM GỠ BOM PANDAS (CHỐNG LỖI MÃ VẠCH)
# ==========================================
def clean_code(val):
    if pd.isna(val): return ''
    if isinstance(val, (int, float)):
        try: return f"{val:.0f}"
        except: pass
    s = str(val).strip().upper()
    if s.endswith('.0'): s = s[:-2]
    if s == 'NAN': return ''
    return s

# ==========================================
# 3. TRANSFORM: LÀM SẠCH VÀ CHUẨN HÓA DỮ LIỆU
# ==========================================
def process_excel_file(filepath):
    print(f"[{filepath}] Đang đọc và xử lý dữ liệu...")
    
    if filepath.lower().endswith('.csv'):
        try: df = pd.read_csv(filepath)
        except UnicodeDecodeError: df = pd.read_csv(filepath, encoding='utf-8-sig') 
    else: df = pd.read_excel(filepath)
    
    df.columns = df.columns.str.strip()

    # 3.1 FILL-DOWN Header (Lấp khoảng trống cho đơn nhiều dòng)
    header_cols = ['ID', 'Thời gian', 'Trạng thái', 'Lý do hủy', 'Nguồn', 'Nhãn', 'Nhân viên bán hàng', 'Giá trị đơn hàng', 'Phí vận chuyển']
    valid_headers = [col for col in header_cols if col in df.columns]
    df[valid_headers] = df[valid_headers].ffill()

    # 3.2 ÉP KIỂU ID
    df['ID'] = pd.to_numeric(df['ID'], errors='coerce').fillna(0).astype(int)
    
    # 3.3 CHUẨN HÓA KHOẢNG TRẮNG CÁC CỘT TEXT
    for col in ['Trạng thái', 'Lý do hủy', 'Nguồn', 'Nhãn', 'Nhân viên bán hàng']:
        if col in df.columns: 
            df[col] = df[col].fillna('').astype(str).str.strip()

    # 🌟 3.4 BỘ LỌC NHÃN ĐỘC QUYỀN
    if 'Nhãn' in df.columns:
        allowed_labels = [
            "FB - LPM Vietnam",
            "FB - LPM Vietnam 168",
            "FB -Blédina Brasses VN",
            "landing page",
            "Web - lepetitmarseillais.vn",
            "OA - LPM Vietnam"
        ]
        df = df[df['Nhãn'].isin(allowed_labels)].copy()
        if df.empty:
            print("   ⚠️ File này không có đơn hàng nào thuộc các Nhãn yêu cầu. Bỏ qua.")
            return df

    # 3.5 XỬ LÝ NGÀY THÁNG
    if 'Thời gian' in df.columns:
        df['Thời gian'] = pd.to_datetime(df['Thời gian'], dayfirst=True, errors='coerce')
        df['order_date'] = df['Thời gian'].dt.date
    else:
        df['Thời gian'], df['order_date'] = pd.NaT, pd.NaT

    # 3.6 CHUẨN HÓA TIỀN & SỐ LƯỢNG
    for col in ['Giá trị đơn hàng', 'Phí vận chuyển', 'Số lượng', 'Giá bán']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').str.replace(' ', '')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 🌟 3.7 LỌC DOANH THU ĐƠN 0 ĐỒNG (QUÀ TẶNG/BẢO HÀNH)
    if 'ID' in df.columns and 'Giá bán' in df.columns and 'Giá trị đơn hàng' in df.columns:
        max_price_per_order = df.groupby('ID')['Giá bán'].transform('max')
        df.loc[max_price_per_order == 0, 'Giá trị đơn hàng'] = 0

    # 3.8 ĐỊNH NGHĨA TRẠNG THÁI HỦY (KHÔNG BAO GỒM "THẤT BẠI")
    if 'Trạng thái' in df.columns:
        cancel_list = ['khách hủy', 'hệ thống hủy', 'hãng vận chuyển hủy đơn', 'đã hoàn']
        df['is_cancelled'] = df['Trạng thái'].str.lower().isin(cancel_list)
    else:
        df['is_cancelled'] = False

    # 3.9 ÉP HÀM LÀM SẠCH VÀO 3 CỘT MÃ
    for col in ['Mã sản phẩm', 'Mã vạch', 'Sản phẩm']:
        if col in df.columns:
            df[col] = df[col].apply(clean_code)

    df['is_gift'] = df['Giá bán'] == 0 if 'Giá bán' in df.columns else False
        
    return df

# ==========================================
# 4. LOAD: NẠP DATABASE & TÍNH TOÁN GIÁ VỐN
# ==========================================
def load_to_db(df):
    if df is None or df.empty:
        return

    print("-> Đang kết nối Database và Import siêu tốc...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # BƯỚC 1: UPSERT ĐƠN HÀNG
        orders_df = df.drop_duplicates(subset=['ID']).copy()
        orders_data = []
        for _, row in orders_df.iterrows():
            if row.get('ID') == 0: continue
            
            o_time = row.get('Thời gian')
            o_date = row.get('order_date')

            orders_data.append((
                str(row.get('ID')).strip(), # BẮT BUỘC ÉP CHUỖI ĐỂ TRÁNH LỖI MAPPING
                CHANNEL_NHANH_ID,
                o_time if pd.notna(o_time) else None,
                o_date if pd.notna(o_date) else None,
                str(row.get('Trạng thái', '')), 
                bool(row.get('is_cancelled', False)),
                str(row.get('Nguồn', '')), 
                str(row.get('Nhãn', '')), 
                str(row.get('Nhân viên bán hàng', '')),
                float(row.get('Giá trị đơn hàng', 0)), 
                float(row.get('Phí vận chuyển', 0))
            ))

        upsert_orders_query = """
            INSERT INTO orders 
            (external_order_id, channel_id, order_time, order_date, status, is_cancelled, source, label, sales_employee, order_value, shipping_fee)
            VALUES %s
            ON CONFLICT (external_order_id, channel_id) DO UPDATE SET
                order_time = EXCLUDED.order_time, order_date = EXCLUDED.order_date, status = EXCLUDED.status,
                is_cancelled = EXCLUDED.is_cancelled, source = EXCLUDED.source, label = EXCLUDED.label,
                sales_employee = EXCLUDED.sales_employee, order_value = EXCLUDED.order_value, shipping_fee = EXCLUDED.shipping_fee,
                updated_at = CURRENT_TIMESTAMP
            RETURNING external_order_id, id;
        """
        
        # 🌟 BƯỚC SỬA LỖI MAPPING TẠI ĐÂY
        returned_ids = execute_values(cursor, upsert_orders_query, orders_data, fetch=True)
        # Đảm bảo dictionary key là chuỗi sạch
        order_mapping = {str(k).strip(): v for k, v in returned_ids}

        # BƯỚC 2: TRA CỨU GIÁ VỐN
        df['internal_order_id'] = df['ID'].astype(str).str.strip().map(order_mapping)
        internal_order_ids = tuple(int(x) for x in df['internal_order_id'].dropna().unique())

        if internal_order_ids:
            all_single_barcodes = set()
            for col in ['Mã vạch', 'Mã sản phẩm', 'Sản phẩm']:
                if col in df.columns:
                    for raw_code in df[col].unique():
                        if raw_code and str(raw_code) != '':
                            parts = [b.strip() for b in str(raw_code).split('+') if b.strip()]
                            all_single_barcodes.update(parts)

            cost_dict = {}
            if all_single_barcodes:
                cursor.execute("SELECT barcode, cost FROM product_costs WHERE barcode IN %s", (tuple(all_single_barcodes),))
                cost_dict = dict(cursor.fetchall())

            # BƯỚC 3: DỌN ITEM CŨ & INSERT ITEM MỚI (LƯỚI LỌC 3 CẤP)
            cursor.execute("DELETE FROM order_items WHERE order_id IN %s", (internal_order_ids,))
            
            items_data = []
            for _, row in df.iterrows():
                if pd.notna(row['internal_order_id']) and row['ID'] != 0:
                    raw_barcode = str(row.get('Mã vạch', ''))
                    raw_product_code = str(row.get('Mã sản phẩm', ''))
                    raw_product_name = str(row.get('Sản phẩm', ''))
                    
                    # Ưu tiên: Mã vạch -> Mã Sản phẩm -> Tên Sản phẩm
                    lookup_code = raw_barcode if raw_barcode else (raw_product_code if raw_product_code else raw_product_name)
                    
                    total_cogs = 0
                    if lookup_code:
                        parts = [b.strip() for b in lookup_code.split('+') if b.strip()]
                        for p in parts:
                            total_cogs += float(cost_dict.get(p, 0))

                    items_data.append((
                        int(row['internal_order_id']), 
                        raw_product_name, 
                        raw_product_code, 
                        raw_barcode,
                        int(row.get('Số lượng', 1)), 
                        float(row.get('Giá bán', 0)), 
                        bool(row.get('is_gift', False)),
                        float(total_cogs)
                    ))

            insert_items_query = """
                INSERT INTO order_items 
                (order_id, product_name, product_code, barcode, quantity, price, is_gift, unit_cogs)
                VALUES %s
            """
            execute_values(cursor, insert_items_query, items_data)

        conn.commit()
        print("✅ Import dữ liệu Nhanh.vn thành công!\n" + "="*40)

    except Exception as e:
        conn.rollback()
        print(f"❌ LỖI TRONG QUÁ TRÌNH IMPORT DATABASE: {e}\n" + "="*40)
        raise e
    finally:
        cursor.close()
        conn.close()

# ==========================================
# 5. KHỐI ĐIỀU KHIỂN CHÍNH
# ==========================================
if __name__ == "__main__":
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.xlsx') or f.endswith('.csv')]
    
    if not files: 
        print(f"⏸ Không tìm thấy file Excel/CSV nào trong thư mục '{INPUT_DIR}'")
    
    for file in files:
        filepath = os.path.join(INPUT_DIR, file)
        try:
            df_processed = process_excel_file(filepath)
            load_to_db(df_processed)
            
            processed_path = os.path.join(PROCESSED_DIR, file)
            os.replace(filepath, processed_path) 
            print(f"📁 Đã chuyển file hoàn tất vào: {processed_path}\n" + "-"*40)
            
        except Exception as e:
            print(f"⚠️ Bỏ qua file '{file}' do gặp sự cố: {e}\n" + "-"*40)