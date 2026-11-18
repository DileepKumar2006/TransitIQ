"""Generate deterministic synthetic transit events; no real-world source data."""
import argparse, csv, random
from datetime import date, timedelta
from pathlib import Path

ROUTES = [("R01","Central Loop"),("R02","Riverside Express"),("R03","University Connector"),("R04","Airport Link"),("R05","Market Street"),("R06","North-South Rapid"),("R07","Harbor Line"),("R08","Tech Park Shuttle"),("R09","Old Town Circular"),("R10","East-West Express")]
STOPS = [("S01","Central Station"),("S02","City Market"),("S03","University"),("S04","Riverside"),("S05","Tech Park"),("S06","Airport Terminal"),("S07","Harbor Gate"),("S08","Old Town")]
def generate(path: Path, rows: int=100_000, seed: int=42):
    random.seed(seed); path.parent.mkdir(parents=True,exist_ok=True); start=date(2025,1,1)
    cols=["date","time","route_id","route_name","stop_id","stop_name","passenger_count","boarding_count","alighting_count","vehicle_id","vehicle_capacity","trip_id","transport_type"]
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(cols)
        for i in range(rows):
            day=start+timedelta(days=random.randrange(365)); hour=random.choices(range(24),weights=[1,1,1,1,1,2,5,9,8,5,4,5,6,5,4,5,8,10,9,6,4,3,2,1])[0]
            route,name=ROUTES[random.randrange(len(ROUTES))]; stop_id,stop=STOPS[random.randrange(len(STOPS))]; capacity=random.choice([40,50,60,75]); peak=1.65 if hour in [7,8,9,17,18,19] else 1.15 if hour in [12,13] else .52; weekend=.72 if day.weekday()>4 else 1
            count=min(capacity+12,max(0,int(random.gauss(26*peak*weekend,9))))
            w.writerow([day.isoformat(),f"{hour:02d}:{random.randrange(60):02d}:00",route,name,stop_id,stop,count,random.randrange(count+1),random.randrange(count+1),f"BUS-{random.randrange(1,61):03d}",capacity,f"T{i+1:07d}","Bus"])
if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--rows",type=int,default=100_000); p.add_argument("--output",type=Path,default=Path(__file__).parent/"sample/transit_sample.csv"); p.add_argument("--seed",type=int,default=42); a=p.parse_args(); generate(a.output,a.rows,a.seed); print(f"Generated {a.rows:,} synthetic records at {a.output}")
