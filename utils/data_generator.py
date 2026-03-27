import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import re
try:
    from faker import Faker
    fake = Faker()
    Faker.seed(42)
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False

np.random.seed(42)
random.seed(42)

FUNDS = [
    {"fund_id": "F001", "fund_name": "Lumina Global Equity Fund",  "domicile": "Luxembourg", "domicile_code": "LU", "currency": "USD", "manager": "Lumina AM",    "legal_form": "SICAV", "regulator": "CSSF"},
    {"fund_id": "F002", "fund_name": "Atlas Fixed Income SICAV",   "domicile": "Ireland",    "domicile_code": "IE", "currency": "EUR", "manager": "Atlas Capital", "legal_form": "ICAV",  "regulator": "CBI"},
    {"fund_id": "F003", "fund_name": "Nexus Multi-Asset Fund",     "domicile": "Luxembourg", "domicile_code": "LU", "currency": "EUR", "manager": "Nexus Group",   "legal_form": "SICAV", "regulator": "CSSF"},
    {"fund_id": "F004", "fund_name": "Orion EM Opportunities",     "domicile": "Cayman",     "domicile_code": "KY", "currency": "USD", "manager": "Orion Partners","legal_form": "LP",    "regulator": "CIMA"},
    {"fund_id": "F005", "fund_name": "Vega Sustainable Growth",    "domicile": "Luxembourg", "domicile_code": "LU", "currency": "EUR", "manager": "Vega ESG AM",   "legal_form": "SICAV", "regulator": "CSSF"},
]
SUB_FUNDS_TEMPLATE = [("Core","Equity"),("Growth","Equity"),("Income","Bond"),("Balanced","Mixed"),("Absolute Return","Alternatives")]
SHARE_CLASS_TYPES = ["A","B","I","R","Z","C"]
COUNTRIES = ["LU","DE","FR","GB","US","CH","IT","ES","NL","BE","SE","DK","AT"]
COUNTRY_NAMES = {"LU":"Luxembourg","DE":"Germany","FR":"France","GB":"United Kingdom","US":"United States","CH":"Switzerland","IT":"Italy","ES":"Spain","NL":"Netherlands","BE":"Belgium","SE":"Sweden","DK":"Denmark","AT":"Austria"}
ASSET_CLASSES = ["Equity","Fixed Income","Real Estate","Commodities","Cash","Derivatives","Private Equity"]
SECTORS = ["Technology","Financials","Healthcare","Consumer Disc.","Industrials","Energy","Materials","Utilities","Real Estate","Comm. Services"]
SECURITIES = [f"SEC{str(i).zfill(4)}" for i in range(1,51)]
SEC_NAMES = ["Apple Inc","Microsoft Corp","Amazon.com","NVIDIA Corp","Alphabet Inc","Meta Platforms","Tesla Inc","Berkshire Hathaway","JP Morgan Chase","UnitedHealth Group","LVMH","ASML Holding","SAP SE","Nestle SA","Roche Holding","TotalEnergies","Siemens AG","BNP Paribas","Airbus SE","Deutsche Telekom","Volkswagen AG","AXA SA","Unilever PLC","BP PLC","Shell PLC","Toyota Motor","Sony Group","Softbank","Samsung Electronics","TSMC","Alibaba Group","Tencent Holdings","JD.com","Baidu Inc","Xiaomi Corp","iShares Core S&P500","Vanguard FTSE All-World","Amundi MSCI EM","SPDR Gold Shares","iShares Euro Gov Bond","US Treasury 10Y","Bund 10Y","OAT 10Y","BTP 10Y","Gilt 10Y","EUR/USD FX Forward","Gold Spot","Brent Crude Future","S&P500 Future","Euro Stoxx 50 Future"]
DOC_TYPES = [{"type":"NAV","sla_hours":2,"frequency":"daily"},{"type":"Portfolio","sla_hours":4,"frequency":"daily"},{"type":"Transaction","sla_hours":3,"frequency":"daily"},{"type":"Registration Matrix","sla_hours":48,"frequency":"monthly"},{"type":"Static Data","sla_hours":24,"frequency":"weekly"},{"type":"AUM Report","sla_hours":6,"frequency":"daily"},{"type":"Risk Report","sla_hours":8,"frequency":"daily"},{"type":"Compliance Report","sla_hours":24,"frequency":"weekly"}]
SOURCES = ["Fund Administrator","Transfer Agent","Custodian","Bloomberg","FactSet","Internal System"]
STEWARD_ROLES = ["Data Owner","Data Steward","Business Owner","Technical Owner","Compliance Officer"]

def gen_stewards(n=12):
    rows = []
    random.seed(42)
    for i in range(n):
        if FAKER_AVAILABLE:
            name = fake.name(); email = fake.email(); dept = fake.job()
        else:
            name = f"User {i+1}"; email = f"user{i+1}@fundgov.com"; dept = random.choice(["Fund Ops","Risk","Compliance","IT","Finance"])
        rows.append({"steward_id":f"STW-{i+1:03d}","name":name,"email":email,"role":random.choice(STEWARD_ROLES),"department":dept,"active":True})
    return pd.DataFrame(rows)

def gen_sub_funds(funds):
    rows = []
    for fund in funds:
        n = random.randint(2,4)
        for i,(name_sfx,strategy) in enumerate(random.sample(SUB_FUNDS_TEMPLATE,n)):
            rows.append({"sub_fund_id":f"{fund['fund_id']}-SF{i+1:02d}","fund_id":fund["fund_id"],"sub_fund_name":f"{fund['fund_name'].split()[0]} {name_sfx}","strategy":strategy,"currency":fund["currency"],"inception_date":(datetime(2018,1,1)+timedelta(days=random.randint(0,1000))).strftime("%Y-%m-%d"),"status":random.choice(["Active","Active","Active","Closed"]),"aum_usd":round(random.uniform(50e6,2e9),0),"domicile":fund["domicile"]})
    return pd.DataFrame(rows)

def gen_share_classes(sub_funds_df):
    rows = []
    fund_domicile = {f["fund_id"]:f["domicile_code"] for f in FUNDS}
    for _,sf in sub_funds_df.iterrows():
        n = random.randint(2,5)
        prefix = fund_domicile.get(sf["fund_id"],"LU")
        for sc_type in random.sample(SHARE_CLASS_TYPES,n):
            currency = random.choice([sf["currency"],"USD","EUR","GBP"])
            isin_suffix = "".join([str(random.randint(0,9)) for _ in range(10)])
            rows.append({"share_class_id":f"{sf['sub_fund_id']}-{sc_type}","sub_fund_id":sf["sub_fund_id"],"fund_id":sf["fund_id"],"share_class_type":sc_type,"isin":f"{prefix}{isin_suffix}","currency":currency,"nav_frequency":random.choice(["Daily","Daily","Daily","Weekly"]),"min_investment":random.choice([1000,5000,10000,100000,1000000]),"mgmt_fee_pct":round(random.uniform(0.25,1.5),2),"status":random.choice(["Active","Active","Active","Inactive"])})
    return pd.DataFrame(rows)

def gen_nav_data(share_classes_df,days=365):
    rows=[]
    end_date=datetime(2026,3,25); start_date=end_date-timedelta(days=days); dates=pd.bdate_range(start_date,end_date)
    for _,sc in share_classes_df.iterrows():
        nav=round(random.uniform(50,500),2); aum=random.uniform(1e6,200e6)
        for dt in dates:
            nav=round(nav*(1+np.random.normal(0.0003,0.008)),4); aum=aum*(1+np.random.normal(0.0001,0.002))
            rows.append({"nav_id":f"{sc['share_class_id']}-{dt.strftime('%Y%m%d')}","share_class_id":sc["share_class_id"],"sub_fund_id":sc["sub_fund_id"],"fund_id":sc["fund_id"],"date":dt.strftime("%Y-%m-%d"),"nav":round(nav,4),"aum":round(aum,0),"currency":sc["currency"],"shares_outstanding":round(aum/nav,0)})
    return pd.DataFrame(rows)

def gen_portfolio(sub_funds_df,days=60):
    rows=[]
    end_date=datetime(2026,3,25); start_date=end_date-timedelta(days=days); dates=pd.bdate_range(start_date,end_date)
    for _,sf in sub_funds_df.iterrows():
        if sf["status"]!="Active": continue
        n_sec=random.randint(15,35); sec_ids=random.sample(range(len(SECURITIES)),n_sec); weights=np.random.dirichlet(np.ones(n_sec)); total_aum=sf["aum_usd"]
        for dt in dates[::5]:
            for i,(sec_idx,w) in enumerate(zip(sec_ids,weights)):
                mv=total_aum*w*np.random.normal(1,0.05); price=round(random.uniform(10,500),2)
                rows.append({"portfolio_id":f"{sf['sub_fund_id']}-{dt.strftime('%Y%m%d')}-{i}","sub_fund_id":sf["sub_fund_id"],"fund_id":sf["fund_id"],"date":dt.strftime("%Y-%m-%d"),"security_id":SECURITIES[sec_idx],"security_name":SEC_NAMES[sec_idx],"asset_class":random.choice(ASSET_CLASSES),"sector":random.choice(SECTORS),"country":random.choice(COUNTRIES),"quantity":round(mv/price,0),"price":price,"market_value_usd":round(mv,2),"weight_pct":round(w*100,4)})
    return pd.DataFrame(rows)

def gen_transactions(sub_funds_df,days=180):
    rows=[]; end_date=datetime(2026,3,25); start_date=end_date-timedelta(days=days)
    tx_types=["Buy","Sell","Subscription","Redemption","Corporate Action","FX Spot","Dividend"]
    for _,sf in sub_funds_df.iterrows():
        for i in range(random.randint(50,200)):
            dt=start_date+timedelta(days=random.randint(0,days)); sec_idx=random.randint(0,len(SECURITIES)-1); qty=round(random.uniform(100,50000),0); price=round(random.uniform(5,600),2)
            rows.append({"tx_id":f"TX-{sf['sub_fund_id']}-{i:04d}","sub_fund_id":sf["sub_fund_id"],"fund_id":sf["fund_id"],"date":dt.strftime("%Y-%m-%d"),"tx_type":random.choice(tx_types),"security_id":SECURITIES[sec_idx],"security_name":SEC_NAMES[sec_idx],"quantity":qty,"price":price,"gross_amount":round(qty*price,2),"currency":sf["currency"],"status":random.choice(["Settled","Settled","Settled","Pending","Failed"]),"broker":random.choice(["Goldman Sachs","JP Morgan","UBS","Deutsche Bank","BNP Paribas","Citi","Morgan Stanley"]),"counterparty":f"CTPY-{random.randint(1,20):03d}"})
    return pd.DataFrame(rows)

def gen_registration_matrix(share_classes_df):
    rows=[]
    for _,sc in share_classes_df.iterrows():
        for country in COUNTRIES:
            is_reg=random.random()>0.35
            rows.append({"share_class_id":sc["share_class_id"],"fund_id":sc["fund_id"],"isin":sc["isin"],"country":country,"country_name":COUNTRY_NAMES[country],"registered":is_reg,"registration_date":(datetime(2018,1,1)+timedelta(days=random.randint(0,2000))).strftime("%Y-%m-%d") if is_reg else None,"expiry_date":(datetime(2026,1,1)+timedelta(days=random.randint(0,730))).strftime("%Y-%m-%d") if is_reg else None,"status":random.choice(["Active","Active","Active","Under Review","Pending"]) if is_reg else "Not Registered"})
    return pd.DataFrame(rows)

def gen_imports(days=90):
    rows=[]; end_date=datetime(2026,3,25); start_date=end_date-timedelta(days=days); dates=pd.bdate_range(start_date,end_date); import_id=1
    for dt in dates:
        for doc in DOC_TYPES:
            if doc["frequency"]=="weekly" and dt.weekday()!=0: continue
            if doc["frequency"]=="monthly" and dt.day!=1: continue
            for fund in FUNDS:
                expected_dt=dt+timedelta(hours=random.randint(14,18)); p=random.random()
                if p<0.75: dh=random.uniform(-0.5,doc["sla_hours"]*0.8); st_="On Time"
                elif p<0.88: dh=doc["sla_hours"]+random.uniform(0,4); st_="Late"
                elif p<0.95: dh=doc["sla_hours"]+random.uniform(4,24); st_="Critical Delay"
                else: dh=None; st_="Missing"
                rows.append({"import_id":f"IMP-{import_id:06d}","date":dt.strftime("%Y-%m-%d"),"fund_id":fund["fund_id"],"fund_name":fund["fund_name"],"doc_type":doc["type"],"frequency":doc["frequency"],"sla_hours":doc["sla_hours"],"expected_time":expected_dt.strftime("%Y-%m-%d %H:%M"),"received_time":(expected_dt+timedelta(hours=dh)).strftime("%Y-%m-%d %H:%M") if dh is not None else None,"delay_hours":round(dh,2) if dh is not None else None,"status":st_,"file_size_kb":round(random.uniform(10,5000),1) if st_!="Missing" else None,"records_count":random.randint(10,50000) if st_!="Missing" else None,"data_quality_score":round(random.uniform(0.75,1.0),3) if st_!="Missing" else None,"source_system":random.choice(SOURCES)}); import_id+=1
    return pd.DataFrame(rows)

def gen_data_quality(sub_funds_df,share_classes_df):
    dimensions=["Completeness","Accuracy","Timeliness","Consistency","Uniqueness","Validity"]
    entities=[{"entity":f["fund_name"],"entity_type":"Fund","layer":"Gold"} for f in FUNDS]+[{"entity":r["sub_fund_name"],"entity_type":"Sub-Fund","layer":"Silver"} for _,r in sub_funds_df.iterrows()]+[{"entity":r["share_class_id"],"entity_type":"Share Class","layer":"Silver"} for _,r in share_classes_df.head(20).iterrows()]
    rows=[]
    for ent in entities:
        for dim in dimensions:
            rows.append({"entity":ent["entity"],"entity_type":ent["entity_type"],"layer":ent["layer"],"dimension":dim,"score":round(random.uniform(0.82,0.99),4),"issues_count":random.randint(0,20),"last_checked":(datetime(2026,3,25)-timedelta(hours=random.randint(0,48))).strftime("%Y-%m-%d %H:%M")})
    return pd.DataFrame(rows)

CATALOG_DESCRIPTIONS = {"NAV":"Daily Net Asset Value per share class. Source of truth: Fund Administrator. Used for investor reporting and regulatory filings.","Portfolio":"Daily holdings positions per sub-fund. Source of truth: Custodian. Includes ISIN, quantity, market value, sector, and geography.","Transaction":"Buys, sells, subscriptions, redemptions, and corporate actions. Source of truth: Custodian + Transfer Agent.","Registration Matrix":"Per-jurisdiction registration status for each share class. Governs which markets a fund can be distributed in.","Static Data":"Fund, Sub-Fund, and Share Class master data. Immutable reference attributes used across all downstream systems.","AUM Report":"Aggregated assets under management per fund and sub-fund. Derived from NAV × shares outstanding.","Risk Report":"Portfolio risk metrics: VaR, tracking error, concentration. Generated by Risk team from portfolio data.","Compliance Report":"Regulatory compliance metrics. Used for UCITS, AIFMD, MiFID II reporting."}
GLOSSARY_TERMS = {"NAV":["nav","net_asset_value"],"AUM":["aum","assets_under_management"],"ISIN":["isin","international_securities_identification_number"],"LEI":["lei","legal_entity_identifier"],"UCITS":["ucits"],"SLA":["sla","service_level_agreement"],"Golden Record":["golden_record","master_record"],"T+1":["t_plus_1","settlement_day"]}

def gen_data_catalog(sub_funds_df,share_classes_df,stewards_df):
    rows=[]; steward_ids=stewards_df["steward_id"].tolist(); steward_names=stewards_df["name"].tolist()
    assets=([{"asset_id":f["fund_id"],"asset_name":f["fund_name"],"asset_type":"Fund","domain":"Static Data","layer":"Gold"} for f in FUNDS]+[{"asset_id":r["sub_fund_id"],"asset_name":r["sub_fund_name"],"asset_type":"Sub-Fund","domain":"Static Data","layer":"Silver"} for _,r in sub_funds_df.iterrows()]+[{"asset_id":r["share_class_id"],"asset_name":r["share_class_id"],"asset_type":"Share Class","domain":"Static Data","layer":"Silver"} for _,r in share_classes_df.head(15).iterrows()]+[{"asset_id":f"NAV-{f['fund_id']}","asset_name":f"NAV - {f['fund_name']}","asset_type":"Dataset","domain":"NAV","layer":"Gold"} for f in FUNDS]+[{"asset_id":f"PORT-{f['fund_id']}","asset_name":f"Portfolio - {f['fund_name']}","asset_type":"Dataset","domain":"Portfolio","layer":"Silver"} for f in FUNDS]+[{"asset_id":f"TX-{f['fund_id']}","asset_name":f"Transactions - {f['fund_name']}","asset_type":"Dataset","domain":"Transaction","layer":"Bronze"} for f in FUNDS])
    for a in assets:
        idx=random.randint(0,len(steward_ids)-1)
        rows.append({"asset_id":a["asset_id"],"asset_name":a["asset_name"],"asset_type":a["asset_type"],"domain":a["domain"],"layer":a["layer"],"description":CATALOG_DESCRIPTIONS.get(a["domain"],"No description."),"owner_id":steward_ids[idx],"owner_name":steward_names[idx],"certification":random.choice(["Certified","Verified","Deprecated","In Review","Draft"]),"pii_flag":random.choice([True,False,False,False]),"sensitive_flag":random.choice([True,False,False]),"glossary_terms":", ".join(random.sample(list(GLOSSARY_TERMS.keys()),random.randint(1,3))),"source_system":random.choice(SOURCES),"last_updated":(datetime(2026,3,25)-timedelta(days=random.randint(0,30))).strftime("%Y-%m-%d"),"popularity_score":round(random.uniform(0.1,1.0),2),"health_score":round(random.uniform(0.70,1.0),2),"row_count":random.randint(100,500000),"column_count":random.randint(5,40),"tags":", ".join(random.sample(["fund-ops","nav","regulatory","risk","esg","aifmd","ucits","mifid","t+1"],random.randint(1,3)))})
    return pd.DataFrame(rows)

LINEAGE_GRAPH=[("Fund Admin NAV Feed","nav_value","Bronze.NAV","nav_raw","Ingest raw","Source","Bronze"),("Bronze.NAV","nav_raw","Silver.NAV","nav_validated","DQ check + null filter","Bronze","Silver"),("Silver.NAV","nav_validated","Gold.NAV","nav_golden","Conflict resolution","Silver","Gold"),("Gold.NAV","nav_golden","Gold.AUM","aum","nav × shares_outstanding","Gold","Gold"),("Custodian Portfolio Feed","position_mv","Bronze.Portfolio","market_value_raw","Ingest raw","Source","Bronze"),("Bronze.Portfolio","market_value_raw","Silver.Portfolio","market_value_usd","Currency conversion + DQ","Bronze","Silver"),("Silver.Portfolio","market_value_usd","Gold.Portfolio","market_value_gold","Sector/ISIN enrichment","Silver","Gold"),("Transfer Agent TX Feed","gross_amount_raw","Bronze.Transaction","gross_amount_raw","Ingest raw","Source","Bronze"),("Bronze.Transaction","gross_amount_raw","Silver.Transaction","gross_amount","Reconcile qty×price","Bronze","Silver"),("Silver.Transaction","gross_amount","Gold.Transaction","gross_amount_net","Net flow aggregation","Silver","Gold"),("Fund Admin Static Feed","share_class_isin","Bronze.StaticData","isin_raw","Ingest raw","Source","Bronze"),("Bronze.StaticData","isin_raw","Silver.StaticData","isin_validated","ISIN format check (BR-005)","Bronze","Silver"),("Silver.StaticData","isin_validated","Gold.StaticData","isin_golden","Dedup + golden record","Silver","Gold"),("Gold.StaticData","isin_golden","Gold.RegMatrix","isin","Registration linkage","Gold","Gold"),("Bloomberg Feed","nav_value","Silver.NAV","nav_bloomberg","Vendor enrichment","Source","Silver"),("FactSet Feed","mgmt_fee","Silver.StaticData","mgmt_fee_pct","Vendor enrichment","Source","Silver")]

def gen_lineage():
    rows=[]
    for i,(sa,sf,ta,tf,tr,lf,lt) in enumerate(LINEAGE_GRAPH):
        rows.append({"lineage_id":f"LIN-{i+1:03d}","source_asset":sa,"source_field":sf,"target_asset":ta,"target_field":tf,"transformation":tr,"layer_from":lf,"layer_to":lt,"last_run":(datetime(2026,3,25)-timedelta(hours=random.randint(0,24))).strftime("%Y-%m-%d %H:%M"),"status":random.choice(["Active","Active","Active","Deprecated"])})
    return pd.DataFrame(rows)

PROFILE_FIELDS={"NAV":[("nav","NUMERIC"),("shares_outstanding","NUMERIC"),("aum","NUMERIC"),("currency","STRING"),("date","DATE")],"Portfolio":[("weight_pct","NUMERIC"),("market_value_usd","NUMERIC"),("price","NUMERIC"),("sector","STRING"),("country","STRING")],"Transaction":[("gross_amount","NUMERIC"),("quantity","NUMERIC"),("price","NUMERIC"),("status","STRING"),("tx_type","STRING")],"Static Data":[("isin","STRING"),("currency","STRING"),("mgmt_fee_pct","NUMERIC"),("status","STRING"),("nav_frequency","STRING")]}

def gen_profiling_stats():
    rows=[]
    for domain,fields in PROFILE_FIELDS.items():
        for (field,dtype) in fields:
            total=random.randint(5000,200000); nulls=random.randint(0,int(total*0.05)); distinct=random.randint(2,min(total,5000))
            rows.append({"domain":domain,"field_name":field,"data_type":dtype,"total_count":total,"null_count":nulls,"null_pct":round(nulls/total*100,2),"distinct_count":distinct,"distinct_pct":round(distinct/total*100,2),"min_value":round(random.uniform(0,10),4) if dtype=="NUMERIC" else None,"max_value":round(random.uniform(100,50000),2) if dtype=="NUMERIC" else None,"mean_value":round(random.uniform(10,1000),2) if dtype=="NUMERIC" else None,"std_dev":round(random.uniform(1,500),2) if dtype=="NUMERIC" else None,"top_value":None if dtype=="NUMERIC" else random.choice(["EUR","USD","Active","Daily"]),"last_profiled":(datetime(2026,3,25)-timedelta(hours=random.randint(0,48))).strftime("%Y-%m-%d %H:%M"),"freshness_ok":random.choice([True,True,True,False])})
    return pd.DataFrame(rows)

if __name__ == "__main__":
    print("data_generator.py OK")
