import numpy as np
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
# Important: Navigate to the correct folder in the Terminal
engine = create_engine("sqlite:////Users/user/Desktop/DataCourse/HW8_sqlalch/sqlalchemy-challenge/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"To search greater than a date or between dates, add a date (YYYY-MM-DD) or dates (YYYY-MM-DD/YYYY-MM-DD) at the end of:<br/>"
        f"/api/v1.0/<start>"
    )
    

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Convert the query results to a dictionary using date as the key and prcp as the value. 
    Return the JSON representation of the dictionary"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all precipitation data with the date
    results = session.query(Measurement.date, Measurement.prcp).all()

    session.close()

    # Create a dictionary from the row data and append to a list 
    all_prcp_data = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict[date] = prcp
        all_prcp_data.append(prcp_dict)

    return jsonify(all_prcp_data)

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all stations
    results = session.query(Station.station).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    """Query the dates and temperature observations of the most active station for the last year of data. 
    Return a JSON list of temperature observations (TOBS) for the previous year"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query measurements for highest used station
    for row in session.query(Measurement).filter(Measurement.station == 'USC00519281').order_by(Measurement.date.desc()).limit(1).all():
        last_date = dt.datetime.strptime(row.date , '%Y-%m-%d')
        year_before_last_date = last_date - dt.timedelta(days=365)
    
    # Perform a query to retrieve the tobs for that station
    tobs_list = []
    for row in session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == 'USC00519281').all():
        date = dt.datetime.strptime(row.date, '%Y-%m-%d')
        if date >= year_before_last_date:
            tobs_list.append(row.tobs)
    
    # Convert list of tuples into normal list
    station_temps = list(np.ravel(tobs_list))

    return jsonify(station_temps)

@app.route("/api/v1.0/<start>")
def start(start):
    """Fetch the start date and greater that matches the path variable supplied by the user, or a 404 if not."""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query tobs data by date and find summary statistics
    check = session.query(Measurement.date).all()

    results = session.query(Measurement.date, func.max(Measurement.tobs), func.min(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).group_by(Measurement.date).all()

    session.close()
    check = list(np.ravel(check))
    summ_tobs = list(np.ravel(results))

    #check if input is a valid date
    if start in check:
        return jsonify(summ_tobs)
    else:
        return jsonify({"error": f"{start} could not be found"}), 404

@app.route("/api/v1.0/<start>/<end>")
def end(start, end):
    """Fetch between the start date and end date supplied by the user, or a 404 if not."""
    #check if input is a valid date
    if start > end:
        return jsonify({"error": f"{start} can't be larger than {end}. Please switch or re-enter dates"}), 404
    else:
        # Create our session (link) from Python to the DB
        session = Session(engine)

        # Query all precipitation data with the date
        check = session.query(Measurement.date).all()

        results = session.query(Measurement.date, func.max(Measurement.tobs), func.min(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start, Measurement.date <= end).group_by(Measurement.date).all()

        session.close()
        check = list(np.ravel(check))
        summ_tobs = list(np.ravel(results))
        #check if input is a valid date
        if start in check:
            return jsonify(summ_tobs)
        else:
            return jsonify({"error": f"{start} could not be found"}), 404

if __name__ == '__main__':
    app.run(debug=True)