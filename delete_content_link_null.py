import json

input_file = "posts_all_dedup.ndjson"
output_file = "posts_all_filtered.ndjson"

count_in = 0
count_out = 0

with open(input_file, "r", encoding="utf-8") as fin, open(output_file, "w", encoding="utf-8") as fout:
    for line in fin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue  # bỏ qua dòng lỗi

        count_in += 1
        content = obj.get("content")
        link = obj.get("link")

        # Loại bỏ nếu không có content và không có link
        if (not link or link.strip() == ""):
            continue

        fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        count_out += 1

print(f"✅ Đã lọc xong {count_in} dòng.")
print(f"➡️ Giữ lại {count_out} dòng có content hoặc link.")
print(f"📁 File mới: {output_file}")
