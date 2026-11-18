from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
import time
import uuid
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

ROOT = Path(__file__).resolve().parents[1]
UPLOADS = ROOT / "backend" / "uploads"
SAMPLE = ROOT / "datasets" / "sample" / "transit_sample.csv"
UPLOADS.mkdir(parents=True, exist_ok=True)
app = FastAPI(title="TransitIQ API", version="1.0.0", description="Public transport analytics; local execution is labeled explicitly.")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
ALIASES = {"date": ["date", "service_date", "trip_date"], "time": ["time", "departure_time", "timestamp"], "route_id": ["route_id", "route", "line_id"], "route_name": ["route_name", "line_name"], "stop_id": ["stop_id", "station_id"], "stop_name": ["stop_name", "station_name"], "passenger_count": ["passenger_count", "passengers", "occupancy"], "boarding_count": ["boarding_count", "boardings"], "alighting_count": ["alighting_count", "alightings"], "vehicle_id": ["vehicle_id", "bus_id"], "vehicle_capacity": ["vehicle_capacity", "capacity"], "trip_id": ["trip_id", "journey_id"], "transport_type": ["transport_type", "mode"]}
REQUIRED = ["date", "time", "route_id", "passenger_count"]
NOTIFICATIONS: list[dict] = []
HISTORY: list[dict] = []

def read_data(dataset: str | None = None) -> pd.DataFrame:
    path = Path(dataset) if dataset else SAMPLE
    resolved = path.resolve()
    if dataset and resolved != SAMPLE.resolve() and UPLOADS.resolve() not in resolved.parents:
        raise HTTPException(403, "Dataset path must be the bundled sample or an uploaded TransitIQ dataset.")
    if not path.exists():
        raise HTTPException(404, "Dataset not found. Generate the sample dataset first.")
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise HTTPException(400, f"Could not read CSV: {exc}")
    # Canonicalize common column variants while preserving original values.
    rename = {}
    lower = {str(c).strip().lower(): c for c in df.columns}
    for canonical, names in ALIASES.items():
        for name in names:
            if name in lower:
                rename[lower[name]] = canonical
                break
    return df.rename(columns=rename)

def filtered(df: pd.DataFrame, start: str | None, end: str | None, route: str | None, mode: str | None) -> pd.DataFrame:
    if start and "date" in df: df = df[df.date.astype(str) >= start]
    if end and "date" in df: df = df[df.date.astype(str) <= end]
    if route and route != "All routes" and "route_id" in df: df = df[df.route_id.astype(str) == route]
    if mode and mode != "All modes" and "transport_type" in df: df = df[df.transport_type.astype(str).str.lower() == mode.lower()]
    return df

def demand_label(value: float, capacity: float | None = None) -> str:
    if capacity and capacity > 0:
        ratio = value / capacity
        return "Overcrowded" if ratio >= .9 else "High demand" if ratio >= .7 else "Moderate demand" if ratio >= .4 else "Low demand"
    return "High demand" if value >= 55 else "Moderate demand" if value >= 28 else "Low demand"

def hourly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=["hour", "passengers"])
    hour = pd.to_datetime(df.time.astype(str), format="mixed", errors="coerce").dt.hour
    if hour.isna().all(): hour = pd.to_numeric(df.time.astype(str).str.extract(r"(\d{1,2})")[0], errors="coerce")
    tmp = df.assign(hour=hour).dropna(subset=["hour"])
    return tmp.groupby("hour", as_index=False).passenger_count.sum().rename(columns={"passenger_count":"passengers"})

def summary(df: pd.DataFrame) -> dict:
    if df.empty: return {"passengers":0,"routes":0,"trips":0,"avg_occupancy":0,"overcrowded_routes":0,"peak_hour":"—","daily":[],"routes_data":[],"hourly":[],"weekday_weekend":[],"recent_activity":[]}
    df = df.copy(); df["passenger_count"] = pd.to_numeric(df.passenger_count, errors="coerce").fillna(0)
    routes = df.groupby(["route_id"] + (["route_name"] if "route_name" in df else []), as_index=False).agg(passengers=("passenger_count","sum"),avg_occupancy=("passenger_count","mean"),trips=("passenger_count","size"))
    cap = pd.to_numeric(df.vehicle_capacity, errors="coerce").mean() if "vehicle_capacity" in df else None
    routes["demand"] = routes.avg_occupancy.apply(lambda n:demand_label(n,cap))
    routes_data = routes.sort_values("passengers",ascending=False).to_dict("records")
    h = hourly(df); peak = int(h.loc[h.passengers.idxmax(),"hour"]) if not h.empty else None
    dates = pd.to_datetime(df.date,errors="coerce") if "date" in df else pd.Series(dtype="datetime64[ns]")
    daily = df.assign(_date=dates.dt.strftime("%Y-%m-%d")).dropna(subset=["_date"]).groupby("_date",as_index=False).passenger_count.sum().rename(columns={"_date":"date","passenger_count":"passengers"}).to_dict("records")
    week = df.assign(_day=dates.dt.day_name()).groupby("_day",as_index=False).passenger_count.sum().rename(columns={"_day":"day","passenger_count":"passengers"}).to_dict("records") if len(dates) else []
    wknd = sum(r["passengers"] for r in week if r["day"] in ["Saturday","Sunday"]); wkdy=sum(r["passengers"] for r in week if r["day"] not in ["Saturday","Sunday"])
    return {"passengers":int(df.passenger_count.sum()),"routes":int(df.route_id.nunique()),"trips":int(df.trip_id.nunique()) if "trip_id" in df else len(df),"avg_occupancy":round(float(df.passenger_count.mean()),1),"overcrowded_routes":sum(r["demand"]=="Overcrowded" for r in routes_data),"peak_hour":f"{peak:02d}:00" if peak is not None else "—","daily":daily,"routes_data":routes_data,"hourly":h.to_dict("records"),"weekday_weekend":[{"name":"Weekday","passengers":int(wkdy)},{"name":"Weekend","passengers":int(wknd)}],"recent_activity":[]}

def hadoop_diagnostic() -> dict:
    cli = shutil.which("hadoop")
    hdfs_cli = shutil.which("hdfs")
    home = Path(os.getenv("HADOOP_HOME") or (Path(cli).resolve().parent.parent if cli else "")) if (os.getenv("HADOOP_HOME") or cli) else None
    core_site = home / "etc" / "hadoop" / "core-site.xml" if home else None
    config_valid = False
    config_error = "Hadoop CLI not found on PATH."
    if core_site and core_site.exists():
        try:
            ET.parse(core_site)
            config_valid = True
            config_error = ""
        except (ET.ParseError, OSError) as exc:
            config_error = f"core-site.xml is invalid: {exc}"
    elif cli:
        config_error = "Hadoop CLI found, but core-site.xml was not found under HADOOP_HOME."
    winutils = bool(home and (home / "bin" / "winutils.exe").exists())
    if config_valid and not winutils:
        config_error = "Hadoop core-site.xml is valid, but Windows winutils.exe is missing."
    usable = bool(cli and hdfs_cli and config_valid and winutils)
    return {"hadoop_cli_found":bool(cli),"hdfs_cli_found":bool(hdfs_cli),"hadoop_config_valid":config_valid,"winutils_found":winutils,"hadoop_prerequisites_ready":usable,"diagnostic":config_error}

@app.get("/api/health")
def health():
    return {"status":"ok","processing_mode":"Local Python / Pandas","sample_available":SAMPLE.exists(),"integrations":{**hadoop_diagnostic(),"spark_cli_found":shutil.which("spark-submit") is not None,"hive_cli_found":shutil.which("hive") is not None}}

@app.get("/api/filters")
def filters(dataset: str | None = None):
    df=read_data(dataset); return {"routes":sorted(df.route_id.dropna().astype(str).unique().tolist()) if "route_id" in df else [],"modes":sorted(df.transport_type.dropna().astype(str).unique().tolist()) if "transport_type" in df else [],"date_min":str(df.date.min()) if "date" in df else None,"date_max":str(df.date.max()) if "date" in df else None}

def get_filtered(dataset,start,end,route,mode): return filtered(read_data(dataset),start,end,route,mode)

@app.get("/api/dashboard")
def dashboard(dataset: str | None=None,start: str | None=None,end: str | None=None,route: str | None=None,mode: str | None=None): return summary(get_filtered(dataset,start,end,route,mode))

@app.get("/api/routes")
def route_analytics(dataset: str | None=None,start: str | None=None,end: str | None=None,route: str | None=None,mode: str | None=None):
    s=summary(get_filtered(dataset,start,end,route,mode)); return {"routes":s["routes_data"],"total_routes":s["routes"]}

@app.get("/api/peak-hours")
def peak_hours(dataset: str | None=None,start: str | None=None,end: str | None=None,route: str | None=None,mode: str | None=None):
    df=get_filtered(dataset,start,end,route,mode); s=summary(df); h=s["hourly"]
    return {"hourly":h,"peak_hour":s["peak_hour"],"weekday_weekend":s["weekday_weekend"],"daily":s["daily"],"busiest_day":max(s["weekday_weekend"],key=lambda x:x["passengers"])["name"] if s["weekday_weekend"] else "—"}

@app.get("/api/demand-heatmap")
def demand_heatmap(dataset: str | None=None):
    df=read_data(dataset)
    if "route_id" not in df or "time" not in df: return {"routes":[],"hours":list(range(24)),"cells":[]}
    parsed=pd.to_datetime(df.time.astype(str),format="mixed",errors="coerce").dt.hour
    if parsed.isna().all(): parsed=pd.to_numeric(df.time.astype(str).str.extract(r"(\d{1,2})")[0],errors="coerce")
    temp=df.assign(hour=parsed).dropna(subset=["hour"])
    cells=temp.groupby(["route_id","hour"],as_index=False).passenger_count.sum()
    return {"routes":sorted(cells.route_id.astype(str).unique().tolist()),"hours":list(range(24)),"cells":[{"route_id":str(r.route_id),"hour":int(r.hour),"passengers":int(r.passenger_count)} for r in cells.itertuples()]}

@app.get("/api/patterns")
def patterns(dataset: str | None=None):
    df=read_data(dataset); out={"top_stops":[],"boardings":int(pd.to_numeric(df.boarding_count,errors="coerce").fillna(0).sum()) if "boarding_count" in df else None,"alightings":int(pd.to_numeric(df.alighting_count,errors="coerce").fillna(0).sum()) if "alighting_count" in df else None,"origin_destination_available":False}
    if "stop_name" in df: out["top_stops"]=df.groupby("stop_name").passenger_count.sum().nlargest(10).rename("passengers").reset_index().to_dict("records")
    return out

@app.get("/api/crowd")
def crowd(dataset: str | None=None,threshold: float=.9):
    if threshold < .1 or threshold > 2: raise HTTPException(422,"threshold must be between 0.1 and 2")
    df=read_data(dataset); s=summary(df); out=[]
    for row in s["routes_data"]:
        cap=float(pd.to_numeric(df.loc[df.route_id.astype(str)==str(row["route_id"]),"vehicle_capacity"],errors="coerce").mean()) if "vehicle_capacity" in df else None
        ratio=(row["avg_occupancy"]/cap) if cap and cap>0 else None
        label="Overcrowded" if ratio is not None and ratio>=threshold else demand_label(row["avg_occupancy"],cap)
        out.append({**row,"capacity":round(cap,1) if cap else None,"occupancy_ratio":round(ratio,3) if ratio is not None else None,"demand":label,"is_estimate":cap is None})
    return {"routes":out,"threshold":threshold,"capacity_available":"vehicle_capacity" in df,"alerts":[{"route_id":r["route_id"],"message":f"{r['demand']} — consider additional service"} for r in out if r["demand"]=="Overcrowded"]}

@app.get("/api/recommendations")
def recommendations(dataset: str | None=None):
    df=read_data(dataset); s=summary(df); rec=[]
    for r in s["routes_data"]:
        part=df[df.route_id.astype(str)==str(r["route_id"])]; h=hourly(part)
        if h.empty: continue
        avg=h.passengers.mean(); peak=h[h.passengers>=avg*1.35].hour.tolist(); low=h[h.passengers<avg*.55].hour.tolist()
        if peak: rec.append({"route_id":r["route_id"],"action":"Add a recommended trip","hours":[f"{int(x):02d}:00" for x in peak],"basis":"Observed hourly passenger demand is at least 35% above this route's hourly mean."})
        if low: rec.append({"route_id":r["route_id"],"action":"Review service frequency","hours":[f"{int(x):02d}:00" for x in low],"basis":"Observed hourly passenger demand is below 55% of this route's hourly mean."})
    return {"status":"recommendations","approval":"Recommendations only; operational approval required.","recommendations":rec}

@app.post("/api/datasets/upload")
async def upload(file: UploadFile=File(...)):
    if not file.filename.lower().endswith(".csv"): raise HTTPException(400,"Upload a CSV file.")
    content=await file.read()
    if len(content)>100*1024*1024: raise HTTPException(413,"CSV exceeds 100 MB limit.")
    try: df=pd.read_csv(io.BytesIO(content))
    except Exception as exc: raise HTTPException(400,f"Invalid CSV: {exc}")
    if len(df.columns)>200: raise HTTPException(400,"CSV has too many columns.")
    path=UPLOADS/f"{uuid.uuid4().hex}_{Path(file.filename).name}"; path.write_bytes(content)
    canonical=read_data(str(path)); missing=[c for c in REQUIRED if c not in canonical.columns]
    return {"id":str(path),"name":file.filename,"records":len(df),"size_bytes":len(content),"columns":list(df.columns),"canonical_columns":list(canonical.columns),"missing_required":missing,"missing_values":{str(k):int(v) for k,v in df.isna().sum().items()},"duplicate_rows":int(df.duplicated().sum()),"preview":df.head(10).fillna("").to_dict("records"),"valid":not missing}

@app.post("/api/datasets/clean")
def clean(dataset: str):
    df=read_data(dataset); before=len(df); df=df.drop_duplicates()
    if "passenger_count" in df: df["passenger_count"]=pd.to_numeric(df.passenger_count,errors="coerce").fillna(0).clip(lower=0)
    path=UPLOADS/f"cleaned_{uuid.uuid4().hex}.csv"; df.to_csv(path,index=False)
    return {"download_url":f"/api/datasets/download?path={path}","records_before":before,"records_after":len(df),"duplicates_removed":before-len(df),"status":"complete"}

@app.get("/api/datasets/download")
def download(path: str):
    p=Path(path).resolve()
    if UPLOADS.resolve() not in p.parents: raise HTTPException(403,"Download path is outside dataset storage.")
    if not p.exists(): raise HTTPException(404,"File not found.")
    return StreamingResponse(open(p,"rb"),media_type="text/csv",headers={"Content-Disposition":f'attachment; filename="{p.name}"'})

@app.get("/api/processing")
def processing_status():
    hadoop=hadoop_diagnostic()
    return {"mode":"Local Python / Pandas","distributed":False,**hadoop,"hadoop_available":hadoop["hadoop_prerequisites_ready"],"hdfs_available":hadoop["hadoop_prerequisites_ready"],"spark_available":shutil.which("spark-submit") is not None,"hive_available":shutil.which("hive") is not None,"history":HISTORY}

@app.post("/api/processing/run")
def processing_run(dataset: str | None=None):
    start=time.perf_counter(); df=read_data(dataset); s=summary(df); elapsed=round(time.perf_counter()-start,4)
    record={"timestamp":datetime.now().isoformat(timespec="seconds"),"technology":"Python / Pandas","mode":"Local","input_records":len(df),"output_records":len(s["routes_data"]),"duration_seconds":elapsed,"status":"completed","results":{"route_totals":s["routes_data"][:10],"hourly_totals":s["hourly"]}}
    HISTORY.insert(0,record); return record

@app.get("/api/hive/queries")
def hive_queries():
    return {"mode":"Reference queries; run with Hive when configured","queries":[{"title":"Passengers by route","sql":"SELECT route_id, SUM(passenger_count) AS passengers FROM transit_trips GROUP BY route_id ORDER BY passengers DESC;"},{"title":"Peak passenger hours","sql":"SELECT hour, SUM(passenger_count) AS passengers FROM transit_trips GROUP BY hour ORDER BY passengers DESC;"},{"title":"Daily statistics","sql":"SELECT service_date, SUM(passenger_count) AS passengers FROM transit_trips GROUP BY service_date ORDER BY service_date;"},{"title":"Crowded routes","sql":"SELECT route_id, AVG(passenger_count / vehicle_capacity) AS occupancy_ratio FROM transit_trips GROUP BY route_id ORDER BY occupancy_ratio DESC;"},{"title":"Demand by weekday","sql":"SELECT dayofweek(service_date), SUM(passenger_count) FROM transit_trips GROUP BY dayofweek(service_date);"}]}

@app.get("/api/notifications")
def notifications():
    if not NOTIFICATIONS:
        try:
            crowd_data=crowd(); now=datetime.now().isoformat(timespec="seconds")
            for r in crowd_data["routes"]:
                if r["demand"] in ["High demand","Overcrowded"]: NOTIFICATIONS.append({"id":str(uuid.uuid4()),"timestamp":now,"message":f"Route {r['route_id']} has {r['demand'].lower()}.","read":False,"severity":"critical" if r["demand"]=="Overcrowded" else "warning"})
            if HISTORY: NOTIFICATIONS.insert(0,{"id":str(uuid.uuid4()),"timestamp":HISTORY[0]["timestamp"],"message":"Local analytics processing completed.","read":False,"severity":"info"})
        except HTTPException: pass
    return {"notifications":NOTIFICATIONS}

@app.post("/api/notifications/{notification_id}/read")
def mark_read(notification_id: str):
    for item in NOTIFICATIONS:
        if item["id"]==notification_id: item["read"]=True; return item
    raise HTTPException(404,"Notification not found.")

@app.get("/api/reports/{kind}")
def report(kind: str,dataset: str | None=None,format: str="csv"):
    if kind not in ["daily","routes","peak","crowd","recommendations"]: raise HTTPException(404,"Unknown report type.")
    data={"daily":summary(read_data(dataset))["daily"],"routes":summary(read_data(dataset))["routes_data"],"peak":peak_hours(dataset)["hourly"],"crowd":crowd(dataset)["routes"],"recommendations":recommendations(dataset)["recommendations"]}[kind]
    df=pd.DataFrame(data)
    if format=="csv": return StreamingResponse(io.BytesIO(df.to_csv(index=False).encode()),media_type="text/csv",headers={"Content-Disposition":f"attachment; filename=transitiq_{kind}.csv"})
    if format=="pdf":
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import letter,landscape
            from reportlab.platypus import SimpleDocTemplate,Table,TableStyle,Paragraph,Spacer
            from reportlab.lib.styles import getSampleStyleSheet
            b=io.BytesIO(); doc=SimpleDocTemplate(b,pagesize=landscape(letter)); rows=[list(df.columns)]+df.fillna("").astype(str).values.tolist()
            table=Table(rows[:200],repeatRows=1); table.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#12304a")),("TEXTCOLOR",(0,0),(-1,0),colors.white),("GRID",(0,0),(-1,-1),.3,colors.lightgrey),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#eef5f8")])]))
            doc.build([Paragraph(f"TransitIQ — {kind.title()} report",getSampleStyleSheet()["Title"]),Spacer(1,16),table]); b.seek(0)
            return StreamingResponse(b,media_type="application/pdf",headers={"Content-Disposition":f"attachment; filename=transitiq_{kind}.pdf"})
        except ImportError: raise HTTPException(501,"Install reportlab to create PDF reports.")
    raise HTTPException(400,"format must be csv or pdf")
