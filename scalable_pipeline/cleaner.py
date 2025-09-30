import pandas as pd
import re

def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    # Quantity
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    
    # Unit price
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0)
    
    # Discount percent (0-1 only)
    df = df[(pd.to_numeric(df["discount_percent"], errors="coerce").fillna(0.0) >= 0)
            & (pd.to_numeric(df["discount_percent"], errors="coerce").fillna(0.0) <= 1)]
    df["discount_percent"] = pd.to_numeric(df["discount_percent"])
    
    # Region standardization
    df["region"] = df["region"].astype(str).str.lower().str.strip().apply(
        lambda x: "north" if re.fullmatch(r"n.*(orth)?", x) else
                  ("south" if re.fullmatch(r"s.*(outh)?", x) else
                  ("east"  if re.fullmatch(r"e.*(ast)?", x) else
                  ("west"  if re.fullmatch(r"w.*(est)?", x) else "unknown"))))
    
    # Category
    df["category"] = (df["category"]
                      .astype(str).str.lower().str.strip()
                      .str.replace(r"[^a-z0-9\s]", "", regex=True)
                      .replace("", "unknown"))
    
    # Product
    df["product_name"] = (df["product_name"]
                          .astype(str).str.lower().str.strip()
                          .str.replace(r"[^a-z0-9\s]", "", regex=True)
                          .replace("", "unknown"))
    
    # Dates
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["month"] = df["sale_date"].dt.to_period("M").astype(str).fillna("unknown")
    
    # Email
    df["customer_email"] = df["customer_email"].fillna("unknown@example.com")
    
    # Revenue
    df["revenue"] = df["quantity"] * df["unit_price"] * (1 - df["discount_percent"])
    
    return df
