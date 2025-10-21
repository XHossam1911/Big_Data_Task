import os, pandas as pd, zipfile

base_dir = "data/bronze_samples"
os.makedirs(base_dir, exist_ok=True)

tables = {
    "accounts.csv": pd.DataFrame({
        "Acc no": ["123","456","123"],
        "Date":   ["01-jan-22","01-feb-22","01-mar-22"],
        "Status": ["Active","Active","In Active"],
    }),
    "account_details.csv": pd.DataFrame({
        "Acc no": ["123","456","123"],
        "Date":   ["01-jan-22","01-feb-22","01-mar-22"],
        "type":   ["CC","Loan","CC"],
    }),
    "person.csv": pd.DataFrame({
        "Acc no": ["123","456","456","123"],
        "Person": ["X","Y","Z","X"],
    }),
    "person_profile.csv": pd.DataFrame({
        "Person": ["X","Y","Z","Z"],
        "Name":   ["Ahmed","Hana","Rana","Rana Ali"],
        "Date":   ["01-jan-22","01-feb-22","01-feb-22","01-mar-22"],
    }),
    "person_iden.csv": pd.DataFrame({
        "Person": ["X","Y","Z","Z"],
        "Id":     ["ID1","ID2","ID3 (NID)","ID4 (PASS)"],
        "Date":   ["01-jan-22","01-feb-22","01-feb-22","01-apr-22"],
    }),
}

zip_path = os.path.join("data", "bronze_sample_tables.zip")
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
    for filename, df in tables.items():
        csv_path = os.path.join(base_dir, filename)
        df.to_csv(csv_path, index=False)
        zf.write(csv_path, arcname=filename)

print(f"✅ Sample CSVs created under: {base_dir}")
print(f"📦 ZIP created at: {zip_path}")
