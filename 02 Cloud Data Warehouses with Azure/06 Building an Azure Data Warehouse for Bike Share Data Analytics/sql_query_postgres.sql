CREATE TABLE rider (rider_id INTEGER PRIMARY KEY,
					first VARCHAR(50),
					last VARCHAR(50),
					address VARCHAR(100),
					birthday DATE,
					account_start_date DATE,
					account_end_date DATE,
					is_member BOOLEAN);

CREATE TABLE payment (payment_id INTEGER PRIMARY KEY,
					  date DATE, 
					  amount MONEY,
					  rider_id INTEGER);
					  
CREATE TABLE station (station_id VARCHAR(50) PRIMARY KEY,
					  name VARCHAR(75), 
					  latitude FLOAT, 
					  longitude FLOAT);
					  
					  
CREATE TABLE trip (trip_id VARCHAR(50) PRIMARY KEY,
				   rideable_type VARCHAR(75),
				   start_at TIMESTAMP,
				   ended_at TIMESTAMP,
				   start_station_id VARCHAR(50),
				   end_station_id VARCHAR(50),
				   rider_id INTEGER);		
				   
				   
