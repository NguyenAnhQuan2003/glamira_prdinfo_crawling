import pandas as pd
import re

df = pd.read_csv("output/domains.csv", header=None, names=["domain"])

pattern = re.compile(r"^(?:www\.)?glamira\.[a-z]{2,}(?:\.[a-z]{2,})?$", re.IGNORECASE)

cleaned = df["domain"].dropna().unique()
valid_domains = sorted(set(filter(pattern.match, cleaned)))

pd.DataFrame(valid_domains, columns=["domain"]).to_csv("output/cleaned_domains.csv", index=False)

print(f"Đã lưu {len(valid_domains)} domain hợp lệ vào 'cleaned_domains.csv'")