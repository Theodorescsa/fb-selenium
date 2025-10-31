import pandas as pd

# ===== CẤU HÌNH =====
input_file = "thoibao-de-last.xlsx"     # đường dẫn file Excel gốc
output_file = "thoibao-de-last-split.xlsx"  # file Excel sau khi chia
num_sheets = 4               # số sheet muốn chia

# ===== ĐỌC FILE =====
df = pd.read_excel(input_file)

# ===== TÍNH SỐ DÒNG MỖI SHEET =====
rows_per_sheet = len(df) // num_sheets
remainder = len(df) % num_sheets

# ===== GHI RA FILE MỚI =====
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    start = 0
    for i in range(num_sheets):
        # nếu không chia hết thì các sheet đầu có thêm 1 dòng
        extra = 1 if i < remainder else 0
        end = start + rows_per_sheet + extra
        chunk = df.iloc[start:end]
        chunk.to_excel(writer, sheet_name=f"Sheet_{i+1}", index=False)
        start = end

print(f"✅ Đã chia {len(df)} dòng thành {num_sheets} sheet đều nhau trong file '{output_file}'.")
