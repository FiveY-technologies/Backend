const express = require('express');
const axios = require('axios');
const mysql = require('mysql');
const app = express();
const port = 3000;
const cors = require('cors');

const apiUrl= 'https://api.vamosys.com/mobile/getGrpDataForTrustedClients?providerName=CALYX&fcode=SPVAMO';

//up change
app.use(cors());

const db = mysql.createPool({
  host: 'fivewhyrds.ctxjvxl0k0dq.us-east-1.rds.amazonaws.com',
  user: 'fivewhyadmin',
  password: 'Yayaya#143',
  database: '5ydatabase'
});

const createTableQueryDirection = `
  CREATE TABLE IF NOT EXISTS Direction (
    id INT NOT NULL,
    dir_latitude NUMERIC(9, 7),
    dir_longitude NUMERIC(9, 7),
    time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(Id)
  )
`;

const createTableQueryFuelData = `
CREATE TABLE IF NOT EXISTS Fueldata (
  id INT PRIMARY KEY,
  timestamp TIMESTAMP,
  organization_name VARCHAR(255),
  vehicle_name VARCHAR(255),
  vehicle_mode VARCHAR(255),
  vehicle_model VARCHAR(255),
  sensor VARCHAR(255),
  current_day_fuel_cost DECIMAL (6,3),
  start_fuel FLOAT,
  fuel_filling FLOAT,
  end_fuel FLOAT,
  fuel_consumption FLOAT,
  fuel_theft FLOAT,
  consumed_fuel_cost DECIMAL (6,3),
  liters_per_hour TIME,
  start_kms FLOAT,
  end_kms FLOAT,
  distance_travelled FLOAT,
  kmpl FLOAT,
  running_hours TIME,
  engine_on_hours TIME,
  secondary_engine_hours TIME,
  engine_idle_hours TIME,
  start_location VARCHAR(255),
  end_location VARCHAR(255),
  driver_name VARCHAR(255),
  driver_mobile_number VARCHAR(255),
  remarks TEXT
)
`;

db.getConnection((connectionError, connection) => {
  if (connectionError) {
    console.error('Database connection failed: ' + connectionError.stack);
    return;
  }

  connection.query(createTableQueryDirection, (err) => {
    if (err) {
      console.error('Error creating the Direction table: ' + err);
    } else {
      console.log('Direction table created successfully');
    }
  });

  connection.query(createTableQueryFuelData, (err) => {
    connection.release();

    if (err) {
      console.error('Error creating the Fueldata table: ' + err);
    } else {
      console.log('Fueldata table created successfully');
    }
  });
});

app.get('/direction', (req, res) => {
  axios.get(apiUrl)
    .then((response) => {
      const apiData = response.data;
      let queriesToExecute = apiData.length;

      db.getConnection((connectionError, connection) => {
        if (connectionError) {
          console.error('Database connection failed: ' + connectionError.stack);
          return res.status(500).json({ error: 'Database connection failed' });
        }

        for (const item of apiData) {
          const checkQuery = 'SELECT id FROM Direction WHERE Id = ?';

          connection.query(checkQuery, [item.rowId], (err, results) => {
            if (err) {
              console.error('Error checking data: ' + err);
            } else if (results.length === 0) {
              const insertQuery = 'INSERT INTO Direction (id, time, dir_latitude, dir_longitude) VALUES (?, CURRENT_TIMESTAMP, ?, ?)';
              const values = [item.rowId, item.latitude, item.longitude];

              connection.query(insertQuery, values, (err) => {
                if (err) {
                  console.error('Error inserting data: ' + err);
                } else {
                  console.log('Data inserted into Direction successfully');
                  logChange(connection, item.rowId, item.latitude, item.longitude);
                }

                queriesToExecute--;
                if (queriesToExecute === 0) {
                  connection.release();
                  fetchAndSendData(res, 'Direction');
                }
              });
            } else {
              const updateQuery = 'UPDATE Direction SET dir_latitude = ?, dir_longitude = ? WHERE id = ?';
              const updateValues = [item.latitude, item.longitude, item.rowId];

              connection.query(updateQuery, updateValues, (err) => {
                if (err) {
                  console.error('Error updating data: ' + err);
                } else {
                  console.log('Data updated in Direction table successfully');
                  logChange(connection, item.rowId, item.latitude, item.longitude);
                }

                queriesToExecute--;
                if (queriesToExecute === 0) {
                  connection.release();
                  fetchAndSendData(res, 'Direction');
                }
                updateValues.length = 0;
              });
            }
          });
        }
      });
    })
    .catch((error) => {
      console.error('Error fetching data from the API: ' + error);
      res.status(500).json({ error: 'Error fetching data from the API' });
    });
});

app.get('/fuelData', (req, res) => {
  axios.get(apiUrl)
    .then((response) => {
      const apiData = response.data;
      let queriesToExecute = apiData.length;

      db.getConnection((connectionError, connection) => {
        if (connectionError) {
          console.error('Database connection failed: ' + connectionError.stack);
          return res.status(500).json({ error: 'Database connection failed' });
        }

        for (const item of apiData) {
          const checkQuery = 'SELECT id FROM Fueldata WHERE id = ?';

          connection.query(checkQuery, [item.rowId], (err, results) => {
            if (err) {
              console.error('Error checking data: ' + err);
            } else if (results.length === 0) {
              const end_fuel = item.fuelLitre - 60;
              const fuel_filling = item.fuelLitre + 20;
              const fuel_consumption = item.fuelLitre + fuel_filling - end_fuel;
              const fuel_theft = fuel_consumption + fuel_filling;
              const current_day_fuel_cost = 103.39;
              const consumed_fuel_cost = current_day_fuel_cost * fuel_consumption;
              const end_kms = item.odoDistance + 10025;
              const distance_travelled = item.overSpeedLimit * 2;
              const kmpl = distance_travelled / fuel_consumption;
              const liters_per_hour = fuel_consumption / 60;

              const values = [
                item.rowId,
                item.orgId,
                item.shortName,
                item.vehicleMode,
                item.vehicleModel,
                item.sensorBasedVehicleMode[0].sensor,
                current_day_fuel_cost,
                item.fuelLitre,
                fuel_filling,
                end_fuel,
                fuel_consumption,
                fuel_theft,
                consumed_fuel_cost,
                liters_per_hour,
                item.odoDistance,
                end_kms,
                distance_travelled,
                kmpl,
                item.todayWorkingHours,
                item.todayWorkingHours,
                item.secondaryEngineHours,
                item.idleTime,
                item.address,
                item.alert,
                item.driverName,
                item.driverMobile,
                item.alert
              ];

              const insertQuery = 'INSERT INTO Fueldata (id, timestamp, organization_name, vehicle_name, vehicle_mode, vehicle_model, sensor, current_day_fuel_cost, start_fuel, fuel_filling, end_fuel, fuel_consumption, fuel_theft, consumed_fuel_cost, liters_per_hour, start_kms, end_kms, distance_travelled, kmpl, running_hours, engine_on_hours, engine_idle_hours, secondary_engine_hours, start_location, end_location, driver_name, driver_mobile_number, remarks) VALUES (?,CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';

              connection.query(insertQuery, values, (err) => {
                if (err) {
                  console.error('Error inserting data: ' + err);
                } else {
                  console.log('Data inserted successfully');
                }

                queriesToExecute--;
                if (queriesToExecute === 0) {
                  connection.release();
                  fetchAndSendData(res, 'Fueldata');
                }
              });
            } else {
              const updateQuery = 'UPDATE Fueldata SET organization_name = ?, vehicle_name = ?, vehicle_mode = ?, vehicle_model = ?, sensor = ?, start_fuel = ?, start_kms = ?, running_hours = ?, engine_on_hours = ?, engine_idle_hours = ?, secondary_engine_hours = ?, start_location = ?, end_location = ?, driver_name = ?, driver_mobile_number = ?, remarks = ? WHERE id = ?';

              const updateValues = [
                item.orgId,
                item.shortName,
                item.vehicleMode,
                item.vehicleModel,
                item.sensorBasedVehicleMode[0].sensor,
                item.fuelLitre,
                item.odoDistance,
                item.todayWorkingHours,
                item.secondaryEngineHours,
                item.todayWorkingHours,
                item.idleTime,
                item.address,
                item.alert,
                item.driverName,
                item.driverMobile,
                item.alert,
                item.rowId
              ];

              connection.query(updateQuery, updateValues, (err) => {
                if (err) {
                  console.error('Error updating data: ' + err);
                } else {
                  console.log('Data updated successfully');
                }

                queriesToExecute--;
                if (queriesToExecute === 0) {
                  connection.release();
                  fetchAndSendData(res, 'Fueldata');
                }

              });
            }
          });
        }
      });
    })
    .catch((error) => {
      console.error('Error fetching data from the API: ' + error);
      res.status(500).json({ error: 'Error fetching data from the API' });
    });
});

app.get('/direction/history', (req, res) => {
    const fromDate = req.query.fromDate;
    const toDate = req.query.toDate;
  
    const selectHistoryQuery = 'SELECT * FROM Direction_History WHERE change_time >= ? AND change_time <= ?';
    const values = [fromDate, toDate];
  
    db.getConnection((connectionError, connection) => {
      if (connectionError) {
        console.error('Database connection failed: ' + connectionError.stack);
        return res.status(500).json({ error: 'Database connection failed' });
      }
  
      connection.query(selectHistoryQuery, values, (err, results) => {
        if (err) {
          console.error('Error fetching data from the Direction_History table: ' + err);
          res.status(500).json({ error: 'Error fetching data from the Direction_History table' });
        } else {
          connection.release();
          res.json(results);
        }
      });
    });
  });

function fetchAndSendData(res, tableName) {
  db.getConnection((connectionError, connection) => {
    if (connectionError) {
      console.error('Database connection failed: ' + connectionError.stack);
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const selectQuery = `SELECT * FROM ${tableName}`;

    connection.query(selectQuery, (err, results) => {
      if (err) {
        console.error('Error fetching data from the database: ' + err);
        res.status(500).json({ error: 'Error fetching data from the database' });
      } else {
        connection.release();
        res.json(results);
      }
    });
  });
}

function logChange(connection, id, dir_latitude, dir_longitude) {
  const logQuery = 'INSERT INTO Direction_History (id, dir_latitude, dir_longitude) VALUES (?, ?, ?)';
  const logValues = [id, dir_latitude, dir_longitude];

  connection.query(logQuery, logValues, (err) => {
    if (err) {
      console.error('Error in logging Direction_History: ' + err);
    } else {
      console.log('Data updated in Direction_History table successfully');
    }
  });
}

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
