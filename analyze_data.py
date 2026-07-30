# -*- coding: utf-8 -*-
import re
import json

file_path = r"E:\文件\启效\3.0\客户项目\中水三立-花凉亭\设备接入\数据接入\入库数据.txt"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

print(f"总行数: {len(lines)}")

# Find all table boundaries
table_starts = []  # (line_no, table_name)

for i, line in enumerate(lines):
    # Match pattern like: t_auto_hltgq_xxx数据：
    m = re.match(r"^(t_auto_hltgq_\S+?)数据[：:]", line.strip())
    if m:
        table_starts.append((i, m.group(1)))

print(f"\n找到 {len(table_starts)} 张表:\n")
for idx, (line_no, table_name) in enumerate(table_starts):
    start_line = line_no + 2  # skip header line
    if idx + 1 < len(table_starts):
        end_line = table_starts[idx + 1][0] - 1
    else:
        end_line = len(lines) - 1
    print(f"  表{idx+1}: {table_name}")
    print(f"    起始行: {line_no+1}, 数据行范围: {start_line+1}-{end_line+1}")
    print(f"    总行数(含空行): {end_line - start_line + 1}")

# For each table, extract: column names, first data row, last data row, count of INSERT rows
print("\n" + "="*80)
print("各表详细分析")
print("="*80)

for idx, (line_no, table_name) in enumerate(table_starts):
    start_line = line_no + 2
    if idx + 1 < len(table_starts):
        end_line = table_starts[idx + 1][0] - 1
    else:
        end_line = len(lines) - 1
    
    print(f"\n{'='*60}")
    print(f"表{idx+1}: {table_name}")
    print(f"{'='*60}")
    
    # Find first INSERT statement to extract columns
    first_insert_line = None
    col_names = ""
    for i in range(line_no, min(line_no + 5, len(lines))):
        if lines[i].strip().startswith("INSERT INTO"):
            first_insert_line = i
            # Extract column names
            m = re.search(r"INSERT INTO .+? \((.+?)\) VALUES", lines[i])
            if m:
                col_names = m.group(1)
                cols = [c.strip().strip('"') for c in col_names.split(",")]
                print(f"  列数: {len(cols)}")
                print(f"  列名: {', '.join(cols)}")
            break
    
    # Count INSERT rows and find first/last VALUES rows
    insert_rows = []
    for i in range(start_line, min(end_line + 1, len(lines))):
        stripped = lines[i].strip()
        if stripped.startswith("INSERT INTO"):
            insert_rows.append(i)
    
    print(f"  INSERT语句数: {len(insert_rows)}")
    
    # Show first INSERT (truncated)
    if insert_rows:
        fi = insert_rows[0]
        line_content = lines[fi].strip()
        # Truncate long lines
        if len(line_content) > 500:
            line_content = line_content[:500] + "...[截断]"
        print(f"  首行INSERT (行{fi+1}):")
        print(f"    {line_content[:400]}")
        
        if len(insert_rows) > 1:
            li = insert_rows[-1]
            line_content = lines[li].strip()
            if len(line_content) > 500:
                line_content = line_content[:500] + "...[截断]"
            print(f"  末行INSERT (行{li+1}):")
            print(f"    {line_content[:400]}")
    
    # Check for any non-INSERT, non-empty lines
    other_lines = []
    for i in range(start_line, min(end_line + 1, len(lines))):
        stripped = lines[i].strip()
        if stripped and not stripped.startswith("INSERT INTO"):
            other_lines.append((i, stripped[:200]))
    if other_lines:
        print(f"  非INSERT行: {len(other_lines)}")
        for ol in other_lines[:5]:
            print(f"    行{ol[0]+1}: {ol[1]}")

print("\n\n分析完成!")
