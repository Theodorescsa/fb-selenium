import json

input_file = "posts_all.ndjson"
output_file = "posts_all_dedup.ndjson"

seen = set()
unique = []

with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue  # bỏ qua dòng lỗi JSON
        key = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            unique.append(obj)

with open(output_file, "w", encoding="utf-8") as f:
    for obj in unique:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

print(f"✅ Giữ lại {len(unique)} object duy nhất từ {len(seen)} dòng NDJSON.")
print(f"➡️ File kết quả: {output_file}")
