from flask import Flask, jsonify
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import func
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import sympy.matrices.benchmarks


app = Flask(__name__)


basedir = r'C:\Users\Juan\bootcamp\Homework\homework_08\data\hawaii.sqlite'
app.config['SQLALCHEMY_DATABASE_URI'] =f"sqlite:///{basedir}?check_same_thread=False"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# app.config['SQLALCHEMY_POOL_RECYCLE'] = 30
db = SQLAlchemy(app)
# Reflect the database into the orm classes:
Base = automap_base()
Base.prepare(db.engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station
Imputed = Base.classes.imputed
# For Usage elsewhere 
date_format = r'%Y-%m-%d'
# Routing Here:
host_port = r'http://127.0.0.1:5000/'
end_point = r'/api/sick_to_thegnar/'
options = ['prcp', 'station', 'tobs']

# A helper function:
def l2str(input_seq, seperator):
       # Join all the strings in list
   str_out = seperator.join(input_seq)
   return(str_out)

# Query Functions:
def last_date(column,date_format):
    ''' Return a datetime object representing the most recent date in the database
    Arguments:
        * table: The table your date column is in 
        * column: Column with dates
        * date_format: string like %Y-%M-%d

    Returns: 
        * : .
    '''
    return datetime.strptime(db.session.query(func.max(column)).first()[0], date_format).date()
    # return datetime.strptime(session.query(Measurement.date).order_by(desc(Measurement.date)).first()[0], date_format).date()
def one_year_ago(column, date_format):
    ''' Returns The date exactly a year prior  to the newest record in a table
    Arguments:
        * table: The table your date column is in 
        * column: Column with dates 
        * date_format: string like %Y-%M-%d
        
    Returns: 
        * year_ago_date:
    '''
    ref_date = last_date(column, date_format)
    #print(f'newest record date: {ref_date}')
    ref_date = ref_date - relativedelta(years=1)
    #print(f'year before newest record: {ref_date}')
    return(ref_date) 

def query_prcp_year(date, table, date_format):
    columns = ['id', 'date', 'prcp']
    prcp_year = db.session.query(table.id, table.date, table.prcp).filter(table.date.between(one_year_ago(table.date, date_format),date)).order_by(table.id)
    prcp_year = pd.DataFrame.from_records(prcp_year.all(),columns=columns)
    return(prcp_year)

def query_tobs_year(date, table, date_format, station):
    columns = ['id', 'date', 'tobs']
    tobs_year = db.session.query(table.id, table.date, table.tobs).filter(table.date.between(one_year_ago(table.date, date_format),date)).filter(table.station==station)
    tobs_year = pd.DataFrame.from_records(tobs_year.all(),columns=columns)
    return tobs_year

#Landing Page with list of endpoints and sample calls:
@app.route("/")
def index():
    terminals = [f'<a href={host_port}{end_point}{options[x]}>{host_port}{end_point}{options[x]}</a>' for x in range(len(options))]
    return(f'{l2str(terminals, "</br>")}')
# Routes:

@app.route(f'{end_point}{options[0]}')
def prcp():
    # This is not thread safe. I developed this query using SQLAlchemy
    #I'm using Flask-Sqlalchemy to do this one... maybe not the 
    df = query_prcp_year(last_date(Measurement.date, date_format), Measurement, date_format)
    return(df[['date', 'prcp']].to_json(orient='records', indent=4))

@app.route(f'{end_point}{options[1]}')
def station():
    columns = ['station' ,'name', 'latitude', 'longitude', 'elevation']
    station_info = db.session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation)
    df = pd.DataFrame.from_records(station_info.all(), columns=columns)
    return(df.to_json(orient='records', indent=4))

@app.route(f'{end_point}{options[2]}')
def tobs():
    columns = ['id', 'date', 'prcp']
    most_active_station = db.session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station))[-1][0]
    df = query_tobs_year(last_date(Measurement.date, date_format), Measurement, date_format, most_active_station)
    return(df[['date', 'tobs']].to_json(orient='records', indent=4))