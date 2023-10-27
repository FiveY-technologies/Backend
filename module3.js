const express = require('express');
const mysql = require('mysql');
const app = express();
const port = 3500;
const axios = require('axios');

const apiUrl = 'https://api.vamosys.com/mobile/getGrpDataForTrustedClients?providerName=CALYX&fcode=SPVAMO';

const db = mysql.createPool({
  host: 'fivewhyrds.ctxjvxl0k0dq.us-east-1.rds.amazonaws.com',
  user: 'fivewhyadmin',
  password: 'Yayaya#143',
  database: '5ydatabase',
});
 //Changes done
const createTableQuery = `
CREATE TABLE IF NOT EXISTS Fueldata (
  id INT PRIMARY KEY,
  timestamp TIMESTAMP,
  organization_name VARCHAR(255),
  vehicle_name VARCHAR(255),
  vehicle_mode VARCHAR(255),
  vehicle_model VARCHAR(255),
  current_day_fuel_cost DECIMAL (6,3),
  consumed_fuel_cost DECIMAL (6,3),
  sensor VARCHAR(255),
  start_fuel FLOAT,
  end_fuel FLOAT,
  fuel_filling FLOAT,
  fuel_theft FLOAT,
  fuel_consumption FLOAT,
  start_kms FLOAT,
  end_kms FLOAT,
  distance_travelled FLOAT,
  kmpl FLOAT,
  running_hours TIME ,
  engine_on_hours TIME,
  secondary_engine_hours TIME,
  engine_idle_hours TIME,
  liters_per_hour TIME,
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

  connection.query(createTableQuery, (err) => {
    connection.release(); 

    if (err) {
      console.error('Error creating the table: ' + err);
    } else {
      console.log('Table created ');
    }
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
              const end_fuel = '20';
              const fuel_filling = '60';
              const fuel_consumption = item.fuelLitre + fuel_filling - end_fuel;
              const fuel_theft  = fuel_consumption + fuel_filling;
              const current_day_fuel_cost = '103.39';
              const consumed_fuel_cost = fuel_consumption * current_day_fuel_cost;
              const end_kms = '3500';
              const distance_travelled = item.odoDistance - end_kms;
              const kmpl = distance_travelled / fuel_consumption;
              const liters_per_hour = '2';
              
              const values = [
                item.rowId, 
                item.orgId, 
                item.shortName, 
                item.vehicleMode, 
                item.vehicleModel, 
                current_day_fuel_cost,
                consumed_fuel_cost,
                item.sensorBasedVehicleMode[0].sensor, 
                item.fuelLitre, 
                end_fuel, 
                fuel_filling, 
                fuel_theft, 
                fuel_consumption, 
                item.odoDistance, 
                end_kms, 
                distance_travelled, 
                kmpl, 
                item.todayWorkingHours,
                item.todayWorkingHours,
                item.secondaryEngineHours, 
                item.idleTime, 
                liters_per_hour, 
                item.address, 
                item.alert, 
                item.driverName, 
                item.driverMobile, 
                item.alert
              ];
             

              const insertQuery = 'INSERT INTO Fueldata (id, timestamp, organization_name, vehicle_name, vehicle_mode, vehicle_model, current_day_fuel_cost, consumed_fuel_cost, sensor, start_fuel, end_fuel, fuel_filling, fuel_theft, fuel_consumption, start_kms, end_kms, distance_travelled, kmpl, running_hours, engine_on_hours, secondary_engine_hours, engine_idle_hours, liters_per_hour, start_location, end_location, driver_name, driver_mobile_number, remarks) VALUES (?,CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';

              connection.query(insertQuery, values, (err) => {
                if (err) {
                  console.error('Error inserting data: ' + err);
                } else {
                  console.log('Data inserted successfully');
                }

                queriesToExecute--;
                if (queriesToExecute === 0) {
                  connection.release(); 
                  fetchAndSendData(res);
                }
              });
            } else {
              
              const updateQuery = 'UPDATE Fueldata SET organization_name = ?, vehicle_name = ?, vehicle_mode = ?, vehicle_model = ?, sensor = ?, start_fuel = ?, start_kms = ?, running_hours = ?, engine_on_hours = ?, secondary_engine_hours = ?, engine_idle_hours = ?, start_location = ?, end_location = ?, driver_name = ?, driver_mobile_number = ?, remarks = ? WHERE id = ?';
              
              const updateValues = [
                item.orgId, 
                item.shortName, 
                item.vehicleMode, 
                item.vehicleModel, 
                item.sensorBasedVehicleMode[0].sensor, 
                item.fuelLitre,           
                item.odoDistance,
                item.todayWorkingHours,
                item.todayWorkingHours,
                item.secondaryEngineHours, 
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
                  fetchAndSendData(res);
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

function fetchAndSendData(res) {
  db.getConnection((connectionError, connection) => {
    if (connectionError) {
      console.error('Database connection failed: ' + connectionError.stack);
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const selectQuery = 'SELECT * FROM Fueldata';

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

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
